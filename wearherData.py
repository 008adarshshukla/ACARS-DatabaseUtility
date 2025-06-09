import xarray as xr
import pandas as pd
import os
import json
from tqdm import tqdm

# File paths
downloads_path = os.path.expanduser("~/Downloads")
file_path = os.path.join(downloads_path, "gfs.t00z.pgrb2.0p25-2.anl")
output_json = os.path.join(downloads_path, "wind_temp_all_heights_profile_11.json")

def strip_time(da):
    return da.isel(time=0) if "time" in da.dims else da

print("📦 Loading datasets...")

# Load only heightAboveGround values
ds_u = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "u", "typeOfLevel": "heightAboveGround"}})
ds_v = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "v", "typeOfLevel": "heightAboveGround"}})
ds_t = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "t", "typeOfLevel": "heightAboveGround"}})

# Strip time if present
u = strip_time(ds_u["u"])
v = strip_time(ds_v["v"])
t = strip_time(ds_t["t"])

# Convert to DataFrames
df_u = u.to_dataframe().reset_index().rename(columns={"u": "u_wind"})
df_v = v.to_dataframe().reset_index().rename(columns={"v": "v_wind"})
df_t = t.to_dataframe().reset_index().rename(columns={"t": "temperature"})

print("🔗 Merging u, v, and temperature data...")

# Merge on latitude, longitude, height
df = df_u.merge(df_v, on=["latitude", "longitude", "heightAboveGround"])
df = df.merge(df_t, on=["latitude", "longitude", "heightAboveGround"])

print(f"📊 Grouping {len(df)} data points by coordinate...")

# Build profile per lat/lon
grouped = df.groupby(["latitude", "longitude"])
output = []

for (lat, lon), group in tqdm(grouped, desc="📈 Building profiles"):
    profile = []
    for _, row in group.sort_values("heightAboveGround").iterrows():
        profile.append({
            "height_m": row["heightAboveGround"],
            "u_wind": row["u_wind"],
            "v_wind": row["v_wind"],
            "temperature_K": row["temperature"]
        })
    output.append({
        "latitude": lat,
        "longitude": lon,
        "profile": profile
    })

print(f"💾 Writing {len(output)} wind+temp profiles to JSON...")

# Save to one JSON file
with open(output_json, "w") as f:
    json.dump(output, f, indent=2)

print(f"✅ Saved complete wind+temperature profiles to:\n{output_json}")
