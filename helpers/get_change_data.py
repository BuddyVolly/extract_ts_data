import json
import time
import pandas as pd
import geopandas as gpd
from pathlib import Path
from godale import Executor
from datetime import timedelta

from helpers.ee.get_time_series import get_time_series
from helpers.ee.util import generate_grid
from helpers.ee.landsat.landsat_collection import landsat_collection
from helpers.ee.ccdc import extract_ccdc

from helpers.ts_analysis.cusum import run_cusum_deforest, cusum_deforest
from helpers.ts_analysis.bfast_wrapper import run_bfast_monitor
from helpers.ts_analysis.bootstrap_slope import run_bs_slope
from helpers.ts_analysis.timescan import run_timescan_metrics
from helpers.ts_analysis.helpers import subset_ts

def get_change_data(aoi, fc, config_dict):
    
    outdir = Path(config_dict['work_dir'])
    outdir.mkdir(parents=True, exist_ok=True)
    config_file = str(outdir.joinpath("config.json"))
    
    with open(config_file, "w") as f:
        json.dump(config_dict, f)

    # create image collection (not being changed)
    lsat = landsat_collection(
        config_dict['ts_params']['start_date'], 
        config_dict['ts_params']['end_date'], 
        aoi, 
        bands=['green', 'red', 'nir', 'swir1', 'swir2', 'ndvi']
    )
    
    def cell_computation(args):
        
        idx, cell, config_file = args
        with open(config_file, "r") as f:
            config_dict = json.load(f)
            
        # check if already been calculated
        if outdir.joinpath(f'tmp_{idx}_results.gpkg').exists():
            print(f' Grid cell {idx} already has been extracted. Going on with next grid cell.')    
            return
        
        # get start time
        start_time = time.time()
        
        # get the timeseries data
        df, nr_of_points = get_time_series(lsat, fc, cell, config_dict)
        
        if nr_of_points > 0:
            if config_dict['ccdc_params']['run']:
                ccdc_df = extract_ccdc(lsat, fc, cell, config_dict)
                df = pd.merge(
                    ccdc_df[['point_id', 'ccdc_change_date', 'ccdc_magnitude']], 
                    df, 
                    on='point_id'
                )

            if config_dict['bfast_params']['run']:
                df = run_bfast_monitor(df, config_dict['bfast_params'])

            ### THINGS WE RUN WITHOUT HISTORIC PERIOD #####

            # we cut ts data to monitoring period only
            df[['dates', 'ts', 'mon_images']] = df.apply(
                lambda row: subset_ts(row, config_dict['ts_params']['start_monitor']), axis=1, result_type='expand'
            )

            if config_dict['cusum_params']['run']:
                df = run_cusum_deforest(df, config_dict['cusum_params'])

            if config_dict['ts_metrics_params']['run']:
                df = run_timescan_metrics(df, config_dict['ts_metrics_params'])

            if config_dict['bs_slope_params']['run']:
                df = run_bs_slope(df, config_dict['bs_slope_params'])

            # if gfc:
                # df = h.extract_gfc_change(df)

            # if tmf:
              # df = h.extract_tmf_change(df)

            gpd.GeoDataFrame(
                df.drop(['dates', 'ts'], axis=1), 
                crs="EPSG:4326", 
                geometry=df['geometry']
            ).to_file(
                outdir.joinpath(f'tmp_{idx}_results.geojson'), driver='GeoJSON'
            )

            # stop timer and print runtime
            elapsed = time.time() - start_time
        
        if nr_of_points > 0:
            print(f' Grid cell {idx} with {nr_of_points} points done in: {timedelta(seconds=elapsed)}')    
        else:
            print(f' Grid cell {idx} does not contain any points. Going on with next grid cell.')    
        
                       
    # create a grid
    grid, grid_fc = generate_grid(aoi, config_dict['ts_params']['grid_size'], config_dict['ts_params']['grid_size'])
    print(f' Parallelizing with {str(config_dict["workers"])} on {len(grid)} grid cells.')
    
    args_list = [(*l, config_file) for l in list(enumerate(grid))]
    
    # ---------------debug line--------------------------
    #cell_computation([5, grid[5], config_file])
    # ---------------debug line end--------------------------
    
    executor = Executor(executor="concurrent_threads", max_workers=config_dict["workers"])
    for i, task in enumerate(executor.as_completed(
        func=cell_computation,
        iterable=args_list
    )):
        try:
            task.result()
        except ValueError:
            print("task failed")
    
    files = list(outdir.glob('*tmp*results.geojson'))
    gdf = gpd.read_file(files[0])
    df = pd.DataFrame(columns=gdf.columns)
    for file in outdir.glob('*tmp*results.geojson'):
        df2 = gpd.read_file(file)
        file.unlink()
        df = pd.concat([df, df2], ignore_index=True)
        
    gpd.GeoDataFrame(
                df, 
                crs="EPSG:4326", 
                geometry=df['geometry']
            ).to_file(
                outdir.joinpath(f'final_results.geojson'), driver='GeoJSON'
            )
