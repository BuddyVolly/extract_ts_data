import ee
from .brdf_correction import apply as apply_brdf


def calculate_ndvi(image):
    return (
        image.addBands(
            image.normalizedDifference(['nir', 'red']).rename('ndvi').multiply(10000).int16()
        ).copyProperties(image)
        .set('system:time_start', image.get('system:time_start'))
        .set('system:footprint', image.get('system:footprint'))
    )


#def add_indices(image):
    
    
    
    
    
    
def bitwiseExtract(value, fromBit, toBit=None):
    if not toBit:
        toBit = fromBit
    maskSize = ee.Number(1).add(toBit).subtract(fromBit)
    mask = ee.Number(1).leftShift(maskSize).subtract(1)
    return value.rightShift(fromBit).bitwiseAnd(mask)


def cloudMaskLsatSR(image):
    qa = image.select('QA_PIXEL')
    cloudShadow = bitwiseExtract(qa, 4)
    snow = bitwiseExtract(qa, 5)
    cloud = bitwiseExtract(qa, 6).Not()
    water = bitwiseExtract(qa, 7)
    return image.updateMask(
        cloudShadow.Not()
            .And(snow.Not())
            .And(cloud.Not())
            .And(water.Not())
    )


def create_collection(collection, start, end, aoi):
    coll = (
        collection
            .filterBounds(aoi)
            .filterDate(start, end)
    )

    return coll.map(cloudMaskLsatSR)


def landsat_collection(start, end, aoi, l8=True, l7=True, l5=True, l4=True, brdf=True, bands="ndvi"):

    coll = None

    if l8:
        # create collection (with masking) and add NDVI
        coll = create_collection(
            ee.ImageCollection("LANDSAT/LC08/C02/T1_L2"), start, end, aoi
        ).select(
        ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
        ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
      )

    if l7:
        # create collection (with masking) and add NDVI
        l7_coll = create_collection(
            ee.ImageCollection(f"LANDSAT/LE07/C02/T1_L2"), start, end, aoi
            ).select(
                ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
                ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
            )

        # merge collection
        coll = coll.merge(l7_coll) if coll else l7_coll

    if l5:
        # create collection (with masking) and add NDVI
        l5_coll = create_collection(
            ee.ImageCollection(f"LANDSAT/LT05/C02/T1_L2"), start, end, aoi
            ).select(
                ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
                ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
            )

        # merge collection
        coll = coll.merge(l5_coll) if coll else l5_coll

    if l4:
        # create collection (with masking) and add NDVI
        l4_coll = create_collection(
            ee.ImageCollection(
                f"LANDSAT/LT04/C02/T1_L2"), start, end, aoi
            ).select(
                ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
                ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
            )

        # merge collection
        coll = coll.merge(l4_coll) if coll else l4_coll

    if brdf:
        coll.map(apply_brdf)

    return coll.map(calculate_ndvi).select(bands)