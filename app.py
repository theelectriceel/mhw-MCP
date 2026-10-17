"""Thin visualization interface; no ETL logic or duplicated analytical formulas."""
import argparse

import pandas as pd
import streamlit as st

from marine_tools import MarineTools

parser = argparse.ArgumentParser()
parser.add_argument("--db", default="data/marine.sqlite")
parser.add_argument("--sources", default="sources.json")
args, _ = parser.parse_known_args()
tools = MarineTools(args.db, args.sources)
st.set_page_config(page_title="Marine data explorer", layout="wide")
st.title("Marine data explorer")
st.caption("Shared pipeline · standardized Parquet · local analysis")
view = st.sidebar.radio("Explore", ["Environmental data", "HAB bulletins", "Ocean colour"])
if view == "Ocean colour":
    from datetime import date
    from marine_tools.ocean_colour import TITLE
    st.subheader(TITLE)
    st.caption("Satellite-derived surface chlorophyll-a concentration (mg/m³). Cloud, ice and land masking can produce missing values. Publication can lag 9–12 months.")
    selected_date = st.date_input("Observation date", value=date(2026, 3, 31), min_value=date(1997, 9, 1), max_value=date.today())
    left, right = st.columns(2)
    latitude = left.number_input("Latitude", min_value=-90.0, max_value=90.0, value=51.675, format="%.3f")
    longitude = right.number_input("Longitude", min_value=-180.0, max_value=180.0, value=-9.625, format="%.3f")
    st.caption("The original global product is preserved. Analysis selects the nearest grid cell within 0.05° on each axis; its actual coordinates appear in the table.")
    st.link_button("Copernicus dataset and licence", "https://cds.climate.copernicus.eu/datasets/satellite-ocean-colour")
    receipt_key = f"ocean-fetch-{selected_date}-{latitude}-{longitude}"
    if st.button("Fetch chlorophyll from source"):
        try:
            with st.spinner("Retrieving the Copernicus product. CDS may queue the request…"):
                from marine_tools.ocean_colour import request_for_date
                receipt = tools.fetch_data("ocean_colour", cds_request=request_for_date(selected_date.year, selected_date.month, selected_date.day), latitude=latitude, longitude=longitude)
            st.session_state[receipt_key] = receipt["fetch_id"]
            st.success("Fetched. Choose ‘Save fetched chlorophyll’ to add it to the cache.")
        except ValueError as exc:
            st.error(str(exc))
    if st.button("Save fetched chlorophyll", disabled=receipt_key not in st.session_state):
        try:
            tools.cache_store(st.session_state[receipt_key])
            st.success("Saved locally. Analysis can now read this selection.")
        except ValueError as exc:
            st.error(str(exc))
    status = tools.pipeline_status()
    if any(d["source"] == "ocean_colour" for d in status["datasets"]):
        try:
            result = tools.pipeline_query("ocean_colour", allow_stale=True)
            st.caption("Last ingested selection below; check its date and grid coordinates when changing the controls.")
            if result["stale"]:
                st.warning("This cached selection has expired. Retrieve it again to refresh.")
            frame = pd.DataFrame(result["rows"])
            st.dataframe(frame, hide_index=True)
            if not frame.empty and frame["value"].isna().all():
                st.warning("The selected cell has no valid chlorophyll estimate for this date.")
            st.download_button("Download chlorophyll records", frame.to_csv(index=False), file_name="ocean_colour.csv", mime="text/csv")
        except ValueError as exc:
            st.warning(str(exc))
    st.stop()
if view == "HAB bulletins":
    import httpx
    from marine_tools.hab_client import HABClient
    st.subheader("Marine Institute HAB bulletins")
    st.caption("Modelled oceanographic information. Issue week does not establish forecast validity dates.")
    year = st.number_input("Bulletin year", min_value=1, max_value=9999, value=2026, step=1)
    week = st.selectbox("Issue week", range(2, 52), format_func=lambda w: f"{w:02d}")
    layout = st.radio("Display", ["Side by side", "Page 12", "Page 13"], horizontal=True)
    client = HABClient()
    receipt_key = f"hab-fetch-{year}-{week}"
    if st.button("Fetch bulletin from source"):
        try:
            fetched = client.fetch(year, week)
            st.session_state[receipt_key] = fetched["fetch_id"]
            st.success("Fetched. Save it to render and cache both pages.")
        except httpx.HTTPError as exc:
            st.warning(f"Fetch failed: {exc}")
    if st.button("Save fetched bulletin", disabled=receipt_key not in st.session_state):
        try:
            client.cache(st.session_state[receipt_key])
            st.success("Both pages are cached locally.")
        except httpx.HTTPError as exc:
            st.warning(f"Cache publication failed: {exc}")
    if st.button("Load cached bulletin"):
        try:
            with st.spinner("Retrieving bulletin pages…"):
                bulletin = client.bulletin(year, week)
                st.caption(f"Cache: {bulletin['cache_status']} · Downloaded: {bulletin['fetched_at']}")
                st.link_button("Original Marine Institute PDF", bulletin["source_url"])
                if bulletin["error_message"]:
                    st.warning(f"Last refresh failed: {bulletin['error_message']}")
                pages = (12, 13) if layout == "Side by side" else (int(layout.split()[-1]),)
                for column, page in zip(st.columns(len(pages)), pages):
                    column.image(client.page(year, week, page), caption=f"Physical PDF page {page}", width="stretch")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                st.warning("This bulletin is not cached or its files are invalid. Fetch it, then save it explicitly.")
            else:
                st.warning(f"Bulletin service returned {exc.response.status_code}: {exc.response.text}")
        except httpx.RequestError:
            st.warning("Cannot reach the bulletin API. Start the local API with run.ps1 serve.")
    st.stop()
fetch_source = st.sidebar.selectbox("Source to fetch", list(tools.list_sources()))
receipt_key = "source-fetch-" + fetch_source
if st.sidebar.button("Fetch from source"):
    try:
        import json
        from pathlib import Path
        plan_path = tools.sources.resolve().parent / "pipeline.json"
        plan = json.loads(plan_path.read_text()) if plan_path.exists() else {}
        with st.spinner("Fetching selected source…"):
            fetched = tools.fetch_data(fetch_source, **plan.get("selections", {}).get(fetch_source, {}))
        st.session_state[receipt_key] = fetched["fetch_id"]
        st.success(f"Fetched {fetched['row_count']:,} observations. Save them explicitly to publish data.")
    except ValueError as exc:
        st.error(str(exc))
if st.sidebar.button("Save fetched data", disabled=receipt_key not in st.session_state):
    try:
        tools.cache_store(st.session_state[receipt_key])
        st.success("Data saved and standardized locally")
    except ValueError as exc:
        st.error(str(exc))
status = tools.pipeline_status()
if not status["datasets"]:
    st.info("Fetch a source, then save the fetched data to get started.")
    st.stop()
st.subheader("Available data")
st.dataframe(status["datasets"], hide_index=True)
source = st.selectbox("Dataset", [d["source"] for d in status["datasets"]],
                      format_func=lambda name: tools.list_sources().get(name, {}).get("name", name))
allow_stale = st.sidebar.checkbox("Allow expired snapshots", value=False)
try:
    dataset = tools.pipeline_query(source, limit=10000, allow_stale=allow_stale)
    frame = pd.DataFrame(dataset["rows"])
    if not frame.empty:
        st.caption(f"Showing {len(frame):,} of {dataset['matched_rows']:,} rows. Source units appear in the unit column.")
        if frame["variable"].nunique() > 1:
            variable = st.selectbox("Measurement", sorted(frame.variable.dropna().unique()))
            frame = frame[frame.variable == variable]
        if frame["time"].notna().any():
            chart = frame.dropna(subset=["time"]).assign(time=lambda f: pd.to_datetime(f.time))
            if chart["species"].notna().any():
                species = st.selectbox("Species to plot", sorted(chart.species.dropna().unique()))
                chart = chart[chart.species == species]
                st.scatter_chart(chart, x="time", y="value", color="location_name")
            elif chart["release_location"].notna().any():
                st.map(chart.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]].dropna())
                st.caption("Particle grid coordinates; values and release locations appear in the table below.")
            else:
                st.line_chart(chart, x="time", y="value")
        st.dataframe(frame, hide_index=True)
        st.download_button("Download displayed records", frame.to_csv(index=False), file_name=source+".csv", mime="text/csv")
    st.subheader("Phytoplankton and thermal exposure")
    days = st.sidebar.slider("Lookback days (including sample date)", 1, 30, 7)
    radius = st.sidebar.slider("Maximum point distance (km)", 1, 100, 20)
    if st.button("Match observations"):
        exposure = tools.analyze_exposure(window_days=days, radius_km=radius, allow_stale=allow_stale)
        st.dataframe(exposure["rows"], hide_index=True)
        if exposure["truncated"]:
            st.warning("Only the first 1,000 matches are displayed. Use date filters through the API for a narrower analysis.")
        st.caption(exposure["limitations"])
        st.json(exposure["method"])
except ValueError as exc:
    st.warning(str(exc))
