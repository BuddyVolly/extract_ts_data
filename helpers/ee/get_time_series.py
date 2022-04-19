import time

import ee
import pandas as pd
import geopandas as gpd
import numpy as np
import requests
from retry import retry

@retry(tries=10, delay=1, backoff=2)
def get_time_series(imageCollection, points, geometry, config_dict):
    
    band = imageCollection.first().bandNames().getInfo()[0]
    point_id_name = config_dict['ts_params']['point_id']
    
    # get geometry of grid cell and filter points for that
    cell = ee.Geometry.Polygon(geometry['coordinates'])
    points = points.filterBounds(geometry)
    nr_of_points = points.size().getInfo()
    if nr_of_points == 0:
        return None, 0
    
    # mask lsat collection for grid cell
    masked_coll = imageCollection.filterBounds(cell)

    # mapping function to extract NDVI time-series from each image
    def mapOverImgColl(image):
        
        geom = image.geometry()
        
        def pixel_value_nan(feature):
            pixel_value = ee.List([feature.get(band), -9999]).reduce(ee.Reducer.firstNonNull())
            return feature.set({'pixel_value': pixel_value, 'imageID': image.id(),})
                
        return image.reduceRegions(
            collection = points.filterBounds(geom),
            reducer = ee.Reducer.first().setOutputs([band]),
            scale = 30            
        ).map(pixel_value_nan)

    # apply mapping ufnciton over landsat collection and get the url of the returned FC
    cell_fc = masked_coll.map(mapOverImgColl).flatten().filter(ee.Filter.neq('pixel_value', -9999));
    url = cell_fc.getDownloadUrl('geojson')

    # Handle downloading the actual pixels.
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise r.raise_for_status()
    
    # write the FC to a geodataframe
    try:
        point_gdf = gpd.GeoDataFrame.from_features(r.json())
    except: # JSONDecodeError:
        return None, -1
        
    if len(point_gdf) > 0:
        return structure_ts_data(point_gdf, point_id_name), nr_of_points
    else:
        return None, 0
    

def structure_ts_data(df, point_id_name):
    
    df.index = pd.DatetimeIndex(pd.to_datetime(df.imageID.apply(lambda x: x.split('_')[-1]), format='%Y%m%d'))
    
    # loop over point_ids and run cusum
    d = {}
    for i, point in enumerate(df[point_id_name].unique()):
        
        # read only orws of points and sort by date
        sub = df[df[point_id_name] == point].sort_index()
        
        #### LANDSAT ONLY ###########
        sub['pathrow'] = sub.imageID.apply(lambda x: x.split('_')[-2])

        # if more than one path row combination covers the point, we select only the one with the most images
        if len(sub.pathrow.unique()) > 1:
            # set an initil length
            length = -1
            # loop through pathrw combinations
            for pathrow in sub.pathrow.unique():
                # check length
                l = len(sub[sub.pathrow == pathrow])
                # compare ot previous length, and if higher reset pathrow and length variable
                if l > length:
                    pr = pathrow
                    length = l
            # finally filter sub df for pathrow with most images
            sub = sub[sub.pathrow == pr]
        #### LANDSAT ONLY ###########
        
        # get the geometry
        geometry = sub.geometry.head(1).values[0]

        # get number of images
        nr_images = len(sub)
        
        # write everything to a dict
        d[i] = dict(
            point_idx=i,
            point_id=point,
            dates=sub.index,
            ts=sub.pixel_value.tolist(),  #### THIS CREATES PROBLEMS FOR DIFFERENT INDICES
            images=nr_images,
            geometry=geometry
        )
    
    # turn the dict into a geodataframe and return
    return  gpd.GeoDataFrame(pd.DataFrame.from_dict(d, orient='index')).set_geometry('geometry')
            