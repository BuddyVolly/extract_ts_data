import numpy as np
from scipy import stats
import pandas as pd
from godale import Executor
    
def calc_timescan_metrics(args):
    
    ts, point_id, outlier_removal, z_threshhold = args
    if outlier_removal:
        z_score = np.abs(stats.zscore(np.array(ts)))
        ts_out = np.ma.MaskedArray(ts, mask=z_score > z_threshhold)
    else:
        ts_out = df.ts
        
    return np.nanmean(ts_out), np.nanstd(ts_out), np.nanmin(ts_out), np.nanmax(ts_out), point_id
    
    
def run_timescan_metrics(df, ts_metrics_params):
    """
    Parallel implementation of the bootstrap slope function
    """
    outlier_removal, z_threshhold = ts_metrics_params['outlier_removal'], ts_metrics_params['z_threshhold']
    args_list, d = [], {}
    for i, row in df.iterrows(): 
        args_list.append([row.ts, row.point_id, outlier_removal, z_threshhold])
        
    executor = Executor(executor="concurrent_threads", max_workers=16)
    for i, task in enumerate(executor.as_completed(
        func=calc_timescan_metrics,
        iterable=args_list
    )):
        try:
            d[i] = list(task.result())
        except ValueError:
            print("task failed")
            
    tscan_df = pd.DataFrame.from_dict(d, orient='index')
    tscan_df.columns = ['ts_mean', 'ts_sd', 'ts_min', 'ts_max', 'point_id']
    return pd.merge(df, tscan_df, on='point_id')    