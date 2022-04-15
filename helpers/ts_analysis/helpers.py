import numpy as np
from datetime import datetime as dt

def subset_ts(row, start_monitor):
    """ Helper function to extract only monitoring period
    """
    
    # create index for monitoring period
    idx = row.dates > dt.strptime(start_monitor, '%Y-%m-%d')
    
    # subset dates
    dates = row.dates[idx]
    
    # subset ts data
    ts = np.array(row.ts)[idx].tolist()
    
    # get new image length
    images = len(ts)
    
    return dates, ts, images