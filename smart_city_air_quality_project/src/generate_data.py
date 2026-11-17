"""Generate deterministic synthetic Delhi NCR air-quality demo data."""
from pathlib import Path
import numpy as np
import pandas as pd

OUT = Path("data/raw/air_quality_raw.csv")
CITIES = {
    "Delhi": (28.6139, 77.2090, 190),
    "Noida": (28.5355, 77.3910, 165),
    "Gurugram": (28.4595, 77.0266, 155),
    "Faridabad": (28.4089, 77.3178, 175),
    "Ghaziabad": (28.6692, 77.4538, 180),
}
rng = np.random.default_rng(42)
rows = []
for city, (lat, lon, baseline) in CITIES.items():
    for dt in pd.date_range("2025-01-01", periods=180, freq="D"):
        seasonal = 45 * np.cos((dt.dayofyear - 15) / 365 * 2 * np.pi)
        temp = 18 + 12 * np.sin((dt.dayofyear - 80) / 365 * 2 * np.pi) + rng.normal(0, 2)
        humidity = np.clip(rng.normal(55, 15), 20, 95)
        wind = np.clip(rng.normal(8, 3), 1, 22)
        pm25 = max(5, rng.normal(baseline * .48 + seasonal * .35, 18))
        pm10 = max(10, rng.normal(baseline * .72 + seasonal * .5, 25))
        no2 = max(2, rng.normal(35 + baseline * .12, 8))
        so2 = max(1, rng.normal(10 + baseline * .025, 3))
        co = max(.1, rng.normal(.8 + baseline * .004, .2))
        aqi = int(np.clip(pm25 * .55 + pm10 * .25 + no2 * .8 + so2 * .5 + co * 15, 15, 500))
        rows.append([dt.date().isoformat(), city, lat, lon, round(temp,1), round(humidity,1),
                     round(wind,1), round(pm25,1), round(pm10,1), round(no2,1),
                     round(so2,1), round(co,2), aqi])
    df = pd.DataFrame(rows, columns=["date","city","latitude","longitude","temperature_c",
        "humidity_pct","wind_speed_kmph","pm25","pm10","no2","so2","co","aqi"])
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
print(f"Wrote {len(df)} synthetic records to {OUT}")
