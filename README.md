# NPMRDS Travel Time Reliability
Python script to calculate NPMRDS Travel Time Reliability from 15-minute-interval travel time data files downloaded from RITIS website.

Total processing time reduced from several hours in TransCAD to 10-20 minutes in Python. Optimization tricks were performed with Pandas to reduce the impact of the big files (3-4GB and ~100 million rows) in memory usage and processing time.

---

Memory optimization with Pandas
  - use pd.to_numeric to downcast numerical columns to smallest numerical dtype possible. source: https://pandas.pydata.org/docs/reference/api/pandas.to_numeric.html
  - use 'category' dtype instead of 'object'.
  - do not create unnecessary columns.
  - avoid .apply and row-wise operations. instead, use vectorized operations.
  - use 'inplace="True"' when available (pd.reset_index, pd.rename, pd.drop, pd.sort_values) instead of assigning copies of the dataframes.

---

How to use it
- Download the data files from RITIS by road type.
  - The main.py is already set for the 4 files: Interstates (all vehicles), Interstates (trucks only), State Routes (all vehicles), and US Routes (all vehicles).
- Extract the CSV files.
- Update the CSV filepaths in main.py.
- Run main.py.

May be updated in future to be executed from DOS or to be a Python library.

---

Requirements
- pandas=2.0.3
- numpy=1.24.3
