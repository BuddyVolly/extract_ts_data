from datetime import datetime as dt

import ee
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from retry import retry

def get_segments(ccdcAst, mask_1d):
    """
    
    """
    bands_2d = ccdcAst.select('.*_coefs').bandNames()
    bands_1d = ccdcAst.bandNames().removeAll(bands_2d)
    segment_1d = ccdcAst.select(bands_1d).arrayMask(mask_1d)
    mask_2d = mask_1d.arrayReshape(ee.Image(ee.Array([-1, 1])), 2)
    segment_2d = ccdcAst.select(bands_2d).arrayMask(mask_2d)
    return segment_1d.addBands(segment_2d)


def get_segment(ccdcAst, mask_1d):
    """
    
    """
    bands_2d = ccdcAst.select('.*_coefs').bandNames()
    bands_1d = ccdcAst.bandNames().removeAll(bands_2d)
    segment_1d = ccdcAst.select(bands_1d).arrayMask(mask_1d).arrayGet([0])
    mask_2d = mask_1d.arrayReshape(ee.Image(ee.Array([-1, 1])), 2)
    segment_2d = ccdcAst.select(bands_2d).arrayMask(mask_2d).arrayProject([1]) 
    return segment_1d.addBands(segment_2d)


def transform_date(date):
    """
    """
    date = pd.to_datetime(dt.fromtimestamp(date/1000.0))
    dates_float = date.year + np.round(date.dayofyear/365, 3)
    dates_float = 0 if dates_float == '1970.003' else dates_float
    return dates_float
    
@retry(tries=10, delay=1, backoff=2)
def extract_ccdc(lsat, points_fc, cell, config_dict):
    
    # extract configuration values
    band = config_dict['ts_params']['band']
    start_monitor = config_dict['ts_params']['start_monitor']
    end = config_dict['ts_params']['end_date']
    point_id_name = config_dict['ts_params']['point_id']
    
    # get geometry of grid cell and filter points for that
    cell = ee.Geometry.Polygon(cell['coordinates'])
    points = points_fc.filterBounds(cell)
    nr_points = points.size().getInfo()
    if nr_points == 0:
        return
    
    # create image collection (not being changed)
    lsat = lsat.filterDate(ee.Date(start_monitor).advance(-2, 'year'), ee.Date(end).advance(2, 'year')).filterBounds(cell)
    
    def get_magnitude(point):

        ccdc = ee.Algorithms.TemporalSegmentation.Ccdc(
            collection=lsat.map(lambda image: image.clip(point)),
            breakpointBands=['green', 'red', 'nir', 'swir1', 'swir2'],
            #breakpointBands=['ndvi'],
            dateFormat=2
          )

        # create array of start of monitoring in shape of tEnd
        tEnd = ccdc.select('tEnd')
        mon_date_array_start = tEnd.multiply(0).add(ee.Date(start_monitor).millis())
        mon_date_array_end = tEnd.multiply(0).add(ee.Date(end).advance(2, 'year').millis())
        # create the date mask
        date_mask = tEnd.gte(mon_date_array_start).And(tEnd.lte(mon_date_array_end))
        # use date mask to mask all of ccdc 
        monitoring_ccdc = get_segments(ccdc, date_mask)

        # mask for highest magnitude in monitoring period
        magnitude = monitoring_ccdc.select(band + '_magnitude')
        max_abs_magnitude = (magnitude
          .abs()
          .arrayReduce(ee.Reducer.max(), [0])
          .arrayGet([0])
          .rename('max_abs_magnitude')
        )

        mask = magnitude.abs().eq(max_abs_magnitude)
        segment = get_segment(monitoring_ccdc, mask)
        magnitude = ee.Image(segment.select(['ndvi_magnitude', 'tBreak', 'tEnd']))

        def ndvi_nan(feature):
            ndvi = ee.List([feature.get('ndvi'), -9999]).reduce(ee.Reducer.firstNonNull())
            return feature.set({'ndvi': ndvi, 'imageID': image.id(),})

        return point.set(magnitude.reduceRegion(
          reducer=ee.Reducer.first(),
          geometry=point.geometry(),
          scale=30
        ))
    
    

    cell_fc = points.map(get_magnitude)
    url = cell_fc.getDownloadUrl('geojson')

    # Handle downloading the actual pixels.
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise r.raise_for_status()

    # write the FC to a geodataframe
    gdf = gpd.GeoDataFrame.from_features(r.json())
    
    gdf['ccdc_change_date'] = gdf['tBreak'].apply(lambda x: transform_date(x))
    gdf['point_id'] = gdf[point_id_name]
    gdf['ccdc_magnitude'] = gdf['ndvi_magnitude']
    return gdf[['ccdc_change_date', 'ccdc_magnitude', 'point_id']]