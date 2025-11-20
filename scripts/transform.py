from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)
PARIS = pytz.timezone("Europe/Paris")

def to_iso_paris(ts_str, fmt_variants=None):
    if ts_str is None:
     return None
    
    if isinstance(ts_str, (int, float)):
       dt = datetime.fromtimestamp(ts_str, tz=PARIS)
       return dt.isoformat()
    
    fmt_variants = fmt_variants or [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
    ]
    for fmt in fmt_variants:
       try:
          dt = datetime.strptime(ts_str, fmt)
          dt = PARIS.localize(dt)
          return dt.isoformat()
       except Exception:
          continue
       logger.warning(f"Impossible de parser la date: {ts_str}")
    return None
       
def celsius_to_kelvin(temp_c):
   if temp_c is None:
      return None
   try:
      return float(temp_c) + 273,15
   except Exception:
      return None
   
def kmh_to_ms(v_kmh):
   if v_kmh is None:
      return None
   try:
      return float(v_kmh) / 3,6
   except Exception:
      return None
   
def flag_and_imput(rows):
   cleaned = []
   events = []
   for r in rows:
        r_clean = r.copy()
        r_clean['date'] = to_iso_paris(r.get('date'))
        if 'temperature' in r:
            r_clean['temperature_k'] = celsius_to_kelvin(r.get('temperature'))
        if 'wind_kmh' in r:
           r_clean['wind_speed_mps'] = kmh_to_ms(r.ge('wind_kmh'))
        try:
            if r_clean.get('wind_speed_mps') is not None and r_clean['wind_speed_mps'] > 100:
               events.append({'type':'outlier','row':r, 'reason':'wind > 100 m/s'})
               r_clean['wind_speed_mps'] = None
        except Exception:
            pass
        cleaned.append(r_clean)
        return cleaned, events