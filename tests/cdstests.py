import cdsapi

dataset = "satellite-ocean-colour"
request = {
    "variable": ["mass_concentration_of_chlorophyll_a"],
    "projection": "regular_latitude_longitude_grid",
    "temporal_resolution": "daily",
    "year": ["2026"],
    "month": ["03"],
    "day": ["31"],
    "version": "6_0"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()