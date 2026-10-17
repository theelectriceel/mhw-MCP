import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marine_tools import MarineTools, ToolError
from marine_tools.eurosea import parse_csv, form
from marine_tools.erddap_csv import parse_csv as parse_erddap
from marine_tools.pipeline import Pipeline


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.config = root / "sources.json"
        self.config.write_text(Path("sources.json").read_text(), encoding="utf-8")
        self.tools = MarineTools(root / "marine.sqlite", self.config, self.fetch)
        self.csv = {
            "sst": "Date,SST (ºC)\n2024-02-28,10\n2024-02-29,14\n2024-03-01,16\n",
            "clim": "Day,PCT.10 (ºC),mean (ºC),PCT.90 (ºC)\n28-Feb,8,9,11\n29-Feb,11,12,13\n01-Mar,12,13,14\n",
            "mhw": "Event ID,start,end,duration (days),intensity (ºC),category\n000,2024-02-28,2024-03-01,3,2.0,Moderate\n",
            "cs": "Event ID,start,end,duration (days),intensity (ºC),category\n",
        }

    def fetch(self, url):
        if "/info/" in url:
            return {"table": {"rows": []}}
        return {"table": {"columnNames": ["time", "latitude", "longitude", "scientific_name", "count", "samplecode"],
                          "columnTypes": ["String", "double", "double", "String", "double", "String"],
                          "columnUnits": ["UTC", "degrees_north", "degrees_east", None, None, None],
                          "rows": [["2024-03-01T12:00:00Z", 51.675, -9.625, "A", 10, "s1"],
                                   ["2024-03-01T12:00:00Z", 51.675, -9.625, "B", 20, "s1"],
                                   ["2024-03-01T12:00:00Z", 55, -9, "A", 30, "far"],
                                   ["2024-03-10T00:00:00Z", 51.675, -9.625, "A", 5, "gap"]]}}

    def download(self, config, folder):
        return parse_csv(self.csv[config["dataset_type"]], config), {}

    def ingest(self):
        with patch("marine_tools.eurosea.download", self.download):
            result = self.tools.pipeline_ingest(sources=["eurosea_sst", "eurosea_clim", "eurosea_mhw", "eurosea_cs", "bantry_phyto"])
        self.assertEqual(result["errors"], [])
        return result

    def test_ingest_parquet_query_idempotent(self):
        self.ingest()
        rows = self.tools.pipeline_query("eurosea_sst", start="2024-02-29", limit=1)
        self.assertEqual(rows["matched_rows"], 2)
        self.assertEqual(rows["rows"][0]["time"], "2024-02-29T00:00:00Z")
        self.assertEqual(rows["rows"][0]["unit"], "degree_Celsius")
        self.assertTrue(Path(rows["provenance"]["path"]).is_file())
        self.assertTrue(all(r["reused"] for r in self.ingest()["datasets"]))
        self.assertEqual(len(self.tools.pipeline_status()["datasets"]), 5)

    def test_spatial_window_leap_day_and_no_row_multiplication(self):
        self.ingest()
        result = self.tools.analyze_exposure(window_days=2, min_coverage=1)
        self.assertEqual(len(result["rows"]), 4)
        near = [r for r in result["rows"] if r["sample_id"] == "s1"]
        self.assertEqual(near[0]["sst_mean_c"], 15)
        self.assertEqual(near[0]["anomaly_mean_c"], 2.5)
        self.assertEqual(near[0]["heatwave_days"], 2)
        self.assertEqual(near[0]["coverage"], 1)
        far = next(r for r in result["rows"] if r["sample_id"] == "far")
        self.assertEqual(far["match_status"], "outside_radius_or_missing_coordinates")
        self.assertIsNone(far["heatwave_days"])
        gap = next(r for r in result["rows"] if r["sample_id"] == "gap")
        self.assertEqual(gap["match_status"], "insufficient_coverage")
        self.assertIsNone(gap["anomaly_mean_c"])
        relationships = self.tools.analyze_relationships(window_days=2)
        self.assertTrue(all(r["pearson_r"] is None for r in relationships["rows"]))

    def test_reject_html_bad_dates_duplicates_and_event_order(self):
        cfg = self.tools.source("eurosea_sst")
        for text in ("<html>error</html>", "Date,SST (ºC)\nbad,3\n", "Date,SST (ºC)\n2024-01-01,3\n2024-01-01,4\n"):
            with self.assertRaises(ToolError):
                parse_csv(text, cfg)
        event = self.csv["mhw"].replace("2024-03-01", "2024-02-01")
        with self.assertRaises(ToolError):
            parse_csv(event, self.tools.source("eurosea_mhw"))
        self.assertEqual(form(cfg), {"/tmp/csv-sst-51.675N-9.625W.csv": "Download SST"})

    def test_csv_particle_endpoint_units_and_missing(self):
        url = self.tools.build_url("bantry_particles_csv")
        self.assertIn("BANTRY_PARTICLES.csv?", url)
        self.assertIn("(last)", url)
        meta = {"table": {"rows": [["dimension", "time", "", "double", ""], ["variable", "particle_hours", "", "double", ""]]}}
        table = parse_erddap("time,particle_hours\nUTC,hours\n2026-10-17T00:00:00Z,NaN\n", meta)["table"]
        self.assertEqual(table["columnTypes"], ["String", "double"])
        self.assertIsNone(table["rows"][0][1])

    def test_partial_failures_and_stale_data(self):
        self.ingest()
        result = self.tools.pipeline_ingest(["unknown_source"])
        self.assertEqual(result["status"], "partial_failure")
        with Pipeline(self.tools).connect() as con:
            con.execute("UPDATE datasets SET expires=0 WHERE source='eurosea_sst'")
        with self.assertRaises(ToolError):
            self.tools.pipeline_query("eurosea_sst")
        self.assertTrue(self.tools.pipeline_query("eurosea_sst", allow_stale=True)["stale"])

    def test_wave_csv_expands_measurements_and_preserves_units(self):
        from urllib.parse import unquote
        url = unquote(self.tools.build_url("bantry_wave_buoy"))
        self.assertIn('IWaveBNetwork.csv?', url)
        self.assertIn('station_id="Bantry Bay"', url)
        self.assertIn('latitude,longitude', url)
        names = ['time', 'station_id', 'latitude', 'longitude', 'SeaTemperature', 'SignificantWaveHeight', 'PeakPeriod', 'PeakDirection']
        meta = {'table': {'rows': [['variable', n, '', 'String' if n == 'station_id' else 'double', ''] for n in names] +
                                  [['attribute', 'SeaTemperature', 'missing_value', 'float', '-999']]}}
        text = ','.join(names) + '\nUTC,,degrees_north,degrees_east,degree_C,cm,s,degrees_true\n' + \
               '2024-09-24T09:30:00Z,Bantry Bay,51.71,-9.7,14,80,6,270\n' + \
               '2024-09-24T10:00:00Z,Bantry Bay,51.71,-9.7,-999,0,NaN,90\n'
        payload = parse_erddap(text, meta)
        self.tools.fetcher = lambda url: copy.deepcopy(meta)
        with patch('marine_tools.erddap_csv.download_csv', return_value=payload):
            result = self.tools.pipeline_ingest(sources=['bantry_wave_buoy'])
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['datasets'][0]['row_count'], 8)
        queried = self.tools.pipeline_query('bantry_wave_buoy', filters=[{'field':'variable','op':'=','value':'significant_wave_height'}])
        self.assertEqual(queried['matched_rows'], 2)
        self.assertEqual([r['value'] for r in queried['rows']], [80, 0])
        self.assertEqual(queried['rows'][0]['unit'], 'cm')
        self.assertEqual(queried['rows'][0]['location_name'], 'Bantry Bay')
        temperature = self.tools.pipeline_query('bantry_wave_buoy', filters=[{'field':'variable','op':'=','value':'sea_temperature'}])
        self.assertIsNone(temperature['rows'][1]['value'])
        self.assertEqual(temperature['rows'][0]['unit'], 'degree_C')


if __name__ == "__main__":
    unittest.main()
