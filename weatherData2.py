
import xarray as xr
import pandas as pd
import os
import json
from tqdm import tqdm
import cfgrib  # required for field listing

# Setup paths
downloads_path = os.path.expanduser("~/Downloads")
file_path = os.path.join(downloads_path, "gfs.t00z.pgrb2.0p25-2.anl")  # adjust if needed
output_json = os.path.join(downloads_path, "wind_temp_height_profiles.json")

def pressure_to_height_m(pressure_hpa):
    """Approximate conversion from pressure (hPa) to height (meters)."""
    return 44330 * (1 - (pressure_hpa / 1013.25) ** 0.1903)

def list_available_fields(filepath):
    """List all fields (shortName, typeOfLevel, level) available in the GRIB file."""
    try:
        index = cfgrib.open_fileindex(filepath)
        return index[['shortName', 'typeOfLevel', 'level']].drop_duplicates()
    except Exception as e:
        print(f"❌ Failed to list fields: {e}")
        return pd.DataFrame()

def load_ds(short_name, type_of_level="isobaricInhPa"):
    """Load a specific variable from GFS file."""
    try:
        backend_kwargs = {"filter_by_keys": {"shortName": short_name}}
        if type_of_level:
            backend_kwargs["filter_by_keys"]["typeOfLevel"] = type_of_level

        ds = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs=backend_kwargs)

        levels = None
        if type_of_level == "isobaricInhPa" and "isobaricInhPa" in ds.coords:
            levels = ds.isobaricInhPa.values
        elif "heightAboveGround" in ds.coords:
            levels = ds.heightAboveGround.values

        print(f"✅ Loaded {short_name} with levels:", levels if levels is not None else "unknown")

        da = ds[short_name]
        if "time" in da.dims:
            da = da.isel(time=0)
        return da.to_dataframe().reset_index().rename(columns={short_name: short_name})
    except Exception as e:
        print(f"❌ Failed to load {short_name} ({type_of_level}): {e}")
        return pd.DataFrame()

# 1. List available fields
print("🔍 Checking available fields in the GRIB file...")
fields = list_available_fields(file_path)
print(fields)

# 2. Load u and v wind from pressure levels
print("📦 Loading u and v wind at pressure levels...")
df_u = load_ds("u", type_of_level="isobaricInhPa")
df_v = load_ds("v", type_of_level="isobaricInhPa")

# 3. Try to load temperature smartly
print("📦 Loading temperature (t)...")
df_t = load_ds("t", type_of_level="isobaricInhPa")

# 3.1. If temperature is empty, try without level filter
if df_t.empty:
    print("⚠️ Temperature not found at isobaric levels. Trying without typeOfLevel...")
    df_t = load_ds("t", type_of_level=None)

# 4. Check if all datasets are loaded
if df_u.empty or df_v.empty or df_t.empty:
    print("❌ One or more datasets could not be loaded. Exiting.")
    exit()

# 5. Merge all three datasets
print("🔗 Merging wind and temperature data...")
merge_on = ["latitude", "longitude"]
if "isobaricInhPa" in df_u.columns:
    merge_on.append("isobaricInhPa")
elif "heightAboveGround" in df_u.columns:
    merge_on.append("heightAboveGround")

df = df_u.merge(df_v, on=merge_on)
df = df.merge(df_t, on=merge_on)

# 6. Group by lat/lon
print("📊 Grouping into vertical profiles...")
output = []
grouped = df.groupby(["latitude", "longitude"])

for (lat, lon), group in tqdm(grouped, desc="📈 Building vertical profiles"):
    profile = []
    if "isobaricInhPa" in group.columns:
        group = group.sort_values("isobaricInhPa", ascending=False)
    elif "heightAboveGround" in group.columns:
        group = group.sort_values("heightAboveGround")

    for _, row in group.iterrows():
        if "isobaricInhPa" in row:
            pressure_hPa = row["isobaricInhPa"]
            height_m = pressure_to_height_m(pressure_hPa)
        else:
            height_m = row["heightAboveGround"]
            pressure_hPa = None

        profile.append({
            "height_m": round(height_m, 2),
            "pressure_hPa": round(pressure_hPa, 2) if pressure_hPa else None,
            "u_wind": round(row["u"], 3),
            "v_wind": round(row["v"], 3),
            "temperature_K": round(row["t"], 2)
        })
    output.append({
        "latitude": round(lat, 5),
        "longitude": round(lon, 5),
        "profile": profile
    })

# 7. Save output to JSON
print("💾 Saving profile data to JSON...")
with open(output_json, "w") as f:
    json.dump(output, f, indent=2)

print(f"✅ Profile data saved to: {output_json}")
