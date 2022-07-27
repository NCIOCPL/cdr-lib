# Adjustments following database refresh of lower tier

After refreshing a lower tier with data from production, it will be necessary to restore the `xxtest` document type and the accompanying permission assigned to the 'Regression Testers' group. To do this, run the `add-xxtest-doctype.py` script.