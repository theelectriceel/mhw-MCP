
import requests

base_url = "https://eurosea.marine.ie/his"

filename = "/tmp/csv-sst-51.675N-9.625W.csv"

response = requests.post(
    base_url + "/his",
    data={filename: "Download SST"}
)

response.raise_for_status()

with open("bantry_sst.csv", "wb") as f:
    f.write(response.content)
