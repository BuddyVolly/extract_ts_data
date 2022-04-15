import numpy as np
import pandas as pd
from datetime import datetime as dt

from bfast import BFASTMonitor
from godale import Executor

# default bFast parameters
defaults = {
    'start_monitor': dt.strptime('2000-01-01', '%Y-%m-%d'), 
    'freq': 365,
    'k': 3, 
    'hfrac':0.25, 
    'trend': False, 
    'level':0.05, 
    'backend':'python'
}

def bfast_monitor(args): 
    """
    Wrapper for BFAST's python implementation
    
    Parameters
    ----------
    
    dates : int
        list of dates for the time-series
    data : float
        list/array of time-series data
    start_monitor : datetime object
        start of the monitoring period
    bfast_params : dict
        dictionary of bfast parameters
        
    Returns
    -----------
    
    bfast_date : float32
        Change Date in fractional year date format
    bfast_magnitude : float32
        Change magnitude of detected break
    bfast_means : float32
        Change confidence of detected break
    """
    
    # unpack args
    data, dates, point_id, bfast_params = args
        
    # initialize model
    params = bfast_params.copy()
    params.update(start_monitor=dt.strptime(bfast_params['start_monitor'], '%Y-%m-%d'))
    del params['run']
    model = BFASTMonitor(**params)
    
    # fit gistorical period
    model.fit(data, dates)
    
    # get monitoring dates
    start_monitor = dt.strptime(bfast_params['start_monitor'], '%Y-%m-%d')
    mon_dates = [date for date in dates if date > start_monitor]
    
    # get breaks in the monitoring period
    bfast_date = mon_dates[model.breaks]
    # transform dates to fractional years
    bfast_date = bfast_date.year + np.round(bfast_date.dayofyear/365, 3)
    
    # get magnitude and means
    bfast_magnitude = model.magnitudes
    bfast_means = model.means
    
    return bfast_date, bfast_magnitude, bfast_means, point_id


def run_bfast_monitor(df, bfast_params):
    """
    Parallel implementation of the bfast_monitor function
    """
    args_list, d = [], {}
    for i, row in df.iterrows():
        args_list.append([row.ts, row.dates, row.point_id, bfast_params])
        
    executor = Executor(executor="concurrent_threads", max_workers=16)
    for i, task in enumerate(executor.as_completed(
        func=bfast_monitor,
        iterable=args_list
    )):
        try:
            d[i] = list(task.result())
        except ValueError:
            print("task failed")
            
    bfast_df = pd.DataFrame.from_dict(d, orient='index')
    bfast_df.columns = ['bfast_change_date', 'bfast_magnitude', 'bfast_means', 'point_id']
    return pd.merge(df, bfast_df, on='point_id')    