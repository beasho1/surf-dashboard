"""
HMB Surf Dashboard Backend
Fetches data from NDBC buoy 46026 (SF), KHAF wind, NOAA tides (Pillar Point),
and Stormsurf NorCal forecast. Updates every 5 minutes via background scheduler.
"""

import requests
import re
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PT = ZoneInfo("America/Los_Angeles")

cache = {
    "wave":     {"data": [], "updated": None, "error": None},
    "wind":     {"data": [], "updated": None, "error": None},
    "tide":     {"predictions": [], "current": None, "trend": None,
                  "updated": None, "error": None},
    "forecast": {"rows": [], "updated": None, "error": None, "url": None},
}

BUOY_URL       = "https://www.ndbc.noaa.gov/data/realtime2/46026.txt"
KHAF_URL       = "https://forecast.weather.gov/data/obhistory/KHAF.html"
TIDE_STATION   = "9414131"
STORMSURF_URL  = "https://www.stormsurf.com/page2/forecast/shortcast/ncal.html"


def fetch_wave_data():
    try:
        resp  = requests.get(BUOY_URL, timeout=15)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if not l.startswith("#")]
        now_utc = datetime.now(timezone.utc)
        cutoff  = now_utc - timedelta(hours=3)
        readings = []
        for line in lines:
            parts = line.split()
            if len(parts) < 12:
                continue
            try:
                dt = datetime(int(parts[0]), int(parts[1]), int(parts[2]),
                              int(parts[3]), int(parts[4]), tzinfo=timezone.utc)
            except ValueError:
                continue
            if dt < cutoff:
                break
            def safe(v):
                return None if v in ("MM","99.0","999.0","9999.0") else round(float(v), 2)
            wvht = safe(parts[8])
            dpd  = safe(parts[9])
            apd  = safe(parts[10])
            mwd  = safe(parts[11])
            if wvht is None and dpd is None and mwd is None:
                continue
            readings.append({
                "time_utc": dt.isoformat(),
                "time_pst": dt.astimezone(PT).strftime("%I:%M %p"),
                "wvht_m":   wvht,
                "wvht_ft":  round(wvht * 3.28084, 1) if wvht else None,
                "dpd_s":    dpd,
                "apd_s":    apd,
                "mwd_deg":  mwd,
            })
        cache["wave"] = {"data": readings, "updated": datetime.now(timezone.utc).isoformat(), "error": None}
        logger.info(f"Wave data updated: {len(readings)} readings")
    except Exception as e:
        logger.error(f"Wave fetch error: {e}")
        cache["wave"]["error"] = str(e)


def fetch_wind_data():
    """Scrape KHAF wind observations from the NWS observation history page."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(KHAF_URL, timeout=15,
                            headers={"User-Agent": "SurfDashboard/1.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the observation table — it has headers like Date, Time, Wind, etc.
        table = None
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if any("wind" in h for h in headers) and any("time" in h for h in headers):
                table = t
                break

        if table is None:
            cache["wind"] = {"data": [], "updated": datetime.now(timezone.utc).isoformat(),
                             "error": "Could not find observation table"}
            return

        now_utc = datetime.now(timezone.utc)
        now_local = datetime.now(PT)
        readings = []

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            try:
                cell_texts = [c.get_text(strip=True) for c in cells]

                # NWS obs history columns vary, but typically:
                # Day | Time | Wind(dir) | Wind(speed) | Wind(gust) | Vis | Weather | ...
                # The wind column sometimes has "Calm" or direction + speed
                day_str = cell_texts[0]
                time_str = cell_texts[1]

                # Parse wind direction and speed
                wind_dir_text = cell_texts[2]
                wind_spd_text = cell_texts[3]
                wind_gst_text = cell_texts[4]

                # Parse speed
                try:
                    wspd = int(wind_spd_text) if wind_spd_text and wind_spd_text not in ("Calm", "NA", "") else 0
                except (ValueError, TypeError):
                    wspd = 0

                # Parse gust
                try:
                    wgst = int(wind_gst_text) if wind_gst_text and wind_gst_text not in ("NA", "", "0") else None
                except (ValueError, TypeError):
                    wgst = None

                # Parse direction
                wdir_card = wind_dir_text if wind_dir_text and wind_dir_text not in ("Calm", "NA", "Vrbl", "") else None
                wdir_deg = cardinal_to_deg(wdir_card) if wdir_card and wdir_card != "Vrbl" else None

                if wdir_card == "Calm" or (wspd == 0 and wdir_card is None):
                    wdir_card = "Calm"
                    wdir_deg = None

                if wdir_card == "Vrbl":
                    wdir_card = "VRB"

                # Build timestamp
                try:
                    day_num = int(day_str)
                    # Parse time like "3:55 PM" or "15:55"
                    if ":" in time_str:
                        time_parts = time_str.replace(" ", "")
                        if "AM" in time_parts.upper() or "PM" in time_parts.upper():
                            t_parsed = datetime.strptime(time_parts, "%I:%M%p")
                        else:
                            t_parsed = datetime.strptime(time_parts, "%H:%M")
                        hour, minute = t_parsed.hour, t_parsed.minute
                    else:
                        continue

                    dt_local = now_local.replace(day=day_num, hour=hour, minute=minute,
                                                 second=0, microsecond=0)
                    if dt_local > now_local:
                        dt_local = dt_local - timedelta(days=1)
                    dt_utc = dt_local.astimezone(timezone.utc)
                except Exception:
                    continue

                # Only keep last 2 hours
                if (now_utc - dt_utc).total_seconds() > 7200:
                    continue

                readings.append({
                    "time_utc":       dt_utc.isoformat(),
                    "time_pst":       dt_local.strftime("%I:%M %p"),
                    "wind_dir_deg":   wdir_deg,
                    "wind_dir_card":  wdir_card if wdir_card else "–",
                    "wind_speed_kt":  wspd,
                    "wind_speed_mph": round(wspd * 1.15078, 1) if wspd else 0,
                    "wind_gust_kt":   wgst,
                    "wind_gust_mph":  round(wgst * 1.15078, 1) if wgst else None,
                })
            except Exception:
                continue

        cache["wind"] = {"data": readings, "updated": datetime.now(timezone.utc).isoformat(), "error": None}
        logger.info(f"Wind data updated: {len(readings)} readings")
    except Exception as e:
        logger.error(f"Wind fetch error: {e}")
        cache["wind"]["error"] = str(e)


def cardinal_to_deg(card):
    """Convert cardinal direction string to degrees."""
    if card is None:
        return None
    dirs = {
        "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
        "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
        "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
        "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
    }
    return dirs.get(card.upper())


def deg_to_cardinal(deg):
    if deg is None:
        return "–"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(deg / 22.5) % 16]


def fetch_tide_data():
    try:
        now_local = datetime.now(PT)
        today     = now_local.strftime("%Y%m%d")
        tomorrow  = (now_local + timedelta(days=1)).strftime("%Y%m%d")
        base      = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

        pred_resp = requests.get(base, params={
            "product": "predictions", "application": "surf_dashboard",
            "begin_date": today, "end_date": tomorrow,
            "datum": "MLLW", "station": TIDE_STATION,
            "time_zone": "lst_ldt", "interval": "hilo",
            "units": "english", "format": "json",
        }, timeout=15)

        wl_resp = requests.get(base, params={
            "product": "water_level", "application": "surf_dashboard",
            "begin_date": today, "end_date": today,
            "datum": "MLLW", "station": TIDE_STATION,
            "time_zone": "lst_ldt", "units": "english", "format": "json",
        }, timeout=15)

        predictions = [{"time": p["t"], "height": float(p["v"]), "type": p["type"]}
                       for p in pred_resp.json().get("predictions", [])]

        # Try real-time water level first
        water_data     = wl_resp.json().get("data", [])
        current_height = None
        for r in reversed(water_data):
            try:
                current_height = round(float(r["v"]), 2)
                break
            except Exception:
                continue

        # Fallback: cosine interpolation between hi/lo predictions
        if current_height is None and len(predictions) >= 2:
            try:
                fmt    = "%Y-%m-%d %H:%M"
                now_dt = now_local.replace(second=0, microsecond=0, tzinfo=None)
                before = after = None
                for p in predictions:
                    p_dt = datetime.strptime(p["time"], fmt)
                    if p_dt <= now_dt:
                        before = (p_dt, p["height"])
                    elif after is None:
                        after = (p_dt, p["height"])
                if before and after:
                    t     = (now_dt - before[0]).total_seconds() / (after[0] - before[0]).total_seconds()
                    cos_t = (1 - math.cos(t * math.pi)) / 2
                    current_height = round(before[1] + (after[1] - before[1]) * cos_t, 2)
                    logger.info(f"Tide height interpolated: {current_height} ft")
            except Exception as e:
                logger.warning(f"Tide interpolation failed: {e}")

        # Determine trend
        trend = None
        if len(water_data) >= 3:
            try:
                recent = [float(r["v"]) for r in water_data[-3:] if r.get("v")]
                if len(recent) >= 2:
                    trend = "Rising" if recent[-1] > recent[0] else "Falling"
            except Exception:
                pass
        if trend is None and predictions:
            fmt    = "%Y-%m-%d %H:%M"
            now_dt = now_local.replace(second=0, microsecond=0, tzinfo=None)
            for p in predictions:
                if datetime.strptime(p["time"], fmt) > now_dt:
                    trend = "Rising" if p["type"] == "H" else "Falling"
                    break

        cache["tide"] = {
            "predictions": predictions, "current": current_height,
            "trend": trend, "updated": datetime.now(timezone.utc).isoformat(), "error": None,
        }
        logger.info(f"Tide data updated: {len(predictions)} predictions, height={current_height}")
    except Exception as e:
        logger.error(f"Tide fetch error: {e}")
        cache["tide"]["error"] = str(e)


def fetch_forecast():
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(STORMSURF_URL, timeout=20,
                            headers={"User-Agent": "Mozilla/5.0 Chrome/122"})
        soup = BeautifulSoup(resp.text, "html.parser")
        days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday","Extended"]
        rows = []
        for tr in soup.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
            if len(cells) >= 5 and any(cells[0].strip().startswith(d) for d in days):
                rows.append({
                    "day": cells[0], "trend": cells[1], "wind": cells[2],
                    "details": cells[3], "waves": cells[4],
                    "direction": cells[5] if len(cells) > 5 else "---",
                })
        cache["forecast"] = {
            "rows": rows, "updated": datetime.now(timezone.utc).isoformat(),
            "error": None, "url": STORMSURF_URL,
        }
        logger.info(f"Forecast updated: {len(rows)} rows")
    except Exception as e:
        logger.error(f"Forecast fetch error: {e}")
        cache["forecast"]["error"] = str(e)


def run_all():
    fetch_wave_data()
    fetch_wind_data()
    fetch_tide_data()
    fetch_forecast()


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/data")
def api_data():
    try:
        updated = cache["wave"].get("updated")
        if updated:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated)).total_seconds()
            if age > 900:
                logger.warning(f"Cache stale ({int(age)}s), triggering refresh")
                import threading
                threading.Thread(target=run_all, daemon=True).start()
    except Exception:
        pass
    return jsonify(cache)

@app.route("/api/refresh", methods=["POST", "GET"])
def manual_refresh():
    run_all()
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


# Run all fetches once at startup
run_all()

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(run_all, "interval", minutes=5,
                  misfire_grace_time=60, coalesce=True, max_instances=1)
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
