import os
import requests
import urllib.request
from datetime import datetime, timezone
from retrying import retry

def get_datetime(yyyy, mm, dd):
    return datetime(yyyy, mm, dd, tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')


def fetch_page_id(page):
    # get id of every item in the page of search results
    return [item["id"] for item in page["features"]]


def fetch_pages_id(session, saved_search_id, num_item=100):
    # How to Paginate:
    # 1) Request a page of search results
    # 2) do something with the page of results
    # 3) if there is more data, recurse and call this method on the next page.
    search_url = ("https://api.planet.com/data/v1/searches/{}" +
                  "/results?_page_size={}").format(saved_search_id, num_item)
    items = []
    i = 0
    while search_url:
        page = session.get(search_url).json()
        new_items = fetch_page_id(page)
        print(f'Page {i}: {len(new_items)} images')
        items += new_items
        search_url = page["_links"].get("_next")
        i += 1
    return items

def get_save_search(search_filter, item_type="PSOrthoTile"):
    session = requests.Session()
    session.auth = (os.environ['PL_API_KEY'], '')
    
    very_large_search = {
        "name": "pathum_thani_search",
        "item_types": [item_type],
        "filter": search_filter
    }
    saved_search = session.post('https://api.planet.com/data/v1/searches/',
                                json=very_large_search)
    print(saved_search)
    saved_search_id = saved_search.json()["id"]
    items = fetch_pages_id(session, saved_search_id)
    return saved_search_id, items

    
if __name__ == "__main__":
    from planet_utils import get_datetime, get_save_search
    from pathum_filter import adm1_pathum_bbox_df as pathum_geometry
    from pathum_filter import get_pathum_filter
    #%% read data one month at a time
    start_date = get_datetime(2019, 1, 1)
    end_date = get_datetime(2019, 6, 1)

    # aoi is the bounding box of Pathum Thani
    aoi_geo = pathum_geometry.__geo_interface__['features'][0]['geometry']
    pathum_filter = get_pathum_filter(start_date, end_date, aoi_geo)

    search_id, img_ids = get_save_search(pathum_filter)