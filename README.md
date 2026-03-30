# CHallenge-48H-data

pip install -r requirements
 Two scripts:

  # 1. Fetch raw data (if not already done)
  python3 fetch_data.py

  # 2. Clean, normalize and join
  python3 clean_normalize.py  
  clean_normalize.py does everything in one shot — cleans both datasets, joins
  them geospatially, and saves 6 files to data/processed/.
