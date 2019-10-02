import geopandas as gpd
import numpy as np
import os
import requests
from requests.auth import HTTPBasicAuth

item_type = "PSOrthoTile"

# get pathum thani boundary
fp1 = "./tha_adm_rtsd_itos_20190221_SHP_PART_1/tha_admbnda_adm1_rtsd_20190221.shp"

# reading the file stored in variables
adm1_df = gpd.read_file(fp1, encoding = 'utf-8') # province
# select only pathum thani
adm1_pathum_df = adm1_df[adm1_df['ADM1_EN'] == 'Pathum Thani'].copy()
# simplify the shape polygon for better performance
adm1_pathum_df.loc[:, 'geometry'] = adm1_pathum_df['geometry'].apply(lambda x: x.simplify(0.00001, preserve_topology=True))
# get geojson format
geo_json_geometry = adm1_pathum_df.__geo_interface__['features'][0]['geometry']

# geo_json_geometry = {
#   "type": "Polygon",
#   "coordinates": [
#       [[100.30654907226562, 13.924736835233936],
#        [100.98220825195311, 13.924736835233936],
#        [100.98220825195311, 14.256397238905413],
#        [100.30654907226562, 14.256397238905413],
#        [100.30654907226562, 13.924736835233936]]
#   ]
# }

def get_pathum_filter(date_start, date_end):
    """ Return the filter for Pathum Thani province 
        collected from date_start to date_end with
        cloud cover less than 10% """
    # filter for items the overlap with our chosen geometry
    geometry_filter = {
      "type": "GeometryFilter",
      "field_name": "geometry",
      "config": geo_json_geometry
    }

    # filter images acquired in a certain date range
    date_range_filter = {
      "type": "DateRangeFilter",
      "field_name": "acquired",
      "config": {
        "gte":date_start,
        "lte":date_end
      }
    }

    # filter any images which are more than 50% clouds
    cloud_cover_filter = {
      "type": "RangeFilter",
      "field_name": "cloud_cover",
      "config": {
        "gte": 0,
        "lte": 0.1
      }
    }

    # create a filter that combines our geo and date filters
    pathum_thani = {
      "type": "AndFilter",
      "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }
    
    return pathum_thani

def get_stats(stat_filter):
    """ Return a date bucketed histogram of number of images by days 
        that matched our filter """
    stats_endpoint_request = {
      "interval": "day",
      "item_types": [item_type],
      "filter": stat_filter
    }
    # fire off the POST request
    return requests.post('https://api.planet.com/data/v1/stats',
                         auth = HTTPBasicAuth(os.environ['PL_API_KEY'], ''),
                         json = stats_endpoint_request)

def get_ids(search_filter):
    """ Return the complete metadata of the matching items """
    search_endpoint_request = {
      "interval": "day",
      "item_types": [item_type],
      "filter": search_filter
    }
    return requests.post('https://api.planet.com/data/v1/quick-search',
                         auth = HTTPBasicAuth(os.environ['PL_API_KEY'], ''),
                         json = search_endpoint_request)


if __name__ == "__main__":
    import os.path
    from geojsonio import display
    
    # save shapefile as GeoJSON using GeoPandas so we can view it with http://geojson.io
    if not os.path.exists("output.json"):
        adm1_pathum_df.to_file("output.json", driver="GeoJSON")
    
    adm1_pathum_df.loc[:, 'geometry'] = adm1_pathum_df['geometry'].apply(lambda x: x.simplify(0.00005, preserve_topology=True))
    display(adm1_pathum_df.to_json())
    
    # test get_stats
    result = get_stats()
    print(result)