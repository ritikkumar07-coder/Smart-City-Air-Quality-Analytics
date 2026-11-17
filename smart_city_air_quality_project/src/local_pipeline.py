"""Local MVP pipeline: raw CSV -> Bronze -> Silver -> Gold."""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/air_quality_raw.csv"
BRONZE = ROOT / "data/bronze/air_quality_bronze.csv"
SILVER = ROOT / "data/silver/air_quality_silver.csv"
GOLD = ROOT / "data/gold"
NUMERIC = ["latitude","longitude","temperature_c","humidity_pct","wind_speed_kmph",
           "pm25","pm10","no2","so2","co","aqi"]
POLLUTANTS = ["pm25","pm10","no2","so2","co"]

def aqi_category(value):
    if value <= 50: return "Good"
    if value <= 100: return "Satisfactory"
    if value <= 200: return "Moderate"
    if value <= 300: return "Poor"
    if value <= 400: return "Very Poor"
    return "Severe"

def main():
    if not RAW.exists():
        raise FileNotFoundError(f"Missing {RAW}. Run python src/generate_data.py first.")
    BRONZE.parent.mkdir(parents=True, exist_ok=True)
    GOLD.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(RAW)
    raw.to_csv(BRONZE, index=False)  # immutable raw landing copy

    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date","city","aqi"])
    df = df.drop_duplicates(subset=["date","city"], keep="last")
    # Fill pollutant/weather nulls with city-level medians, then global medians as fallback.
    for col in NUMERIC:
        df[col] = df.groupby("city")[col].transform(lambda s: s.fillna(s.median()))
        df[col] = df[col].fillna(df[col].median())
    # Basic validity filters
    df = df[df["aqi"].between(0, 500)]
    for col in POLLUTANTS:
        df = df[df[col] >= 0]
    df["aqi_category"] = df["aqi"].apply(aqi_category)
    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.to_csv(SILVER, index=False)

    silver = pd.read_csv(SILVER, parse_dates=["date"])
    daily = (silver.groupby(["date","city","latitude","longitude"], as_index=False)
             .agg(avg_aqi=("aqi","mean"), avg_pm25=("pm25","mean"),
                  avg_pm10=("pm10","mean"), avg_temperature_c=("temperature_c","mean"),
                  avg_humidity_pct=("humidity_pct","mean"), records=("aqi","count")))
    daily["aqi_category"] = daily["avg_aqi"].apply(aqi_category)
    summary = (silver.groupby("city", as_index=False)
               .agg(avg_aqi=("aqi","mean"), max_aqi=("aqi","max"),
                    avg_pm25=("pm25","mean"), avg_pm10=("pm10","mean"),
                    avg_no2=("no2","mean"), avg_so2=("so2","mean"),
                    avg_co=("co","mean"), observations=("aqi","count")))
    summary["aqi_category"] = summary["avg_aqi"].apply(aqi_category)
    pollutant = silver[POLLUTANTS].mean().rename_axis("pollutant").reset_index(name="average_value")
    monthly = (silver.groupby(["year_month","city"], as_index=False)
               .agg(avg_aqi=("aqi","mean"), max_aqi=("aqi","max")))
    daily.to_csv(GOLD/"city_daily_aqi.csv", index=False)
    summary.to_csv(GOLD/"city_summary.csv", index=False)
    pollutant.to_csv(GOLD/"pollutant_summary.csv", index=False)
    monthly.to_csv(GOLD/"monthly_trend.csv", index=False)
    print(f"Pipeline complete. Raw={len(raw)}, Silver={len(silver)}")
    print(summary.sort_values("avg_aqi", ascending=False).to_string(index=False))

if __name__ == "__main__":
    main()
