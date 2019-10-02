import os
import requests
import urllib.request
from datetime import datetime, timezone
from retrying import retry

EXCEED_QUOTA = 403 # used to be 429 from changelog May 2019

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


def retry_if_asset_ok(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return not isinstance(exception, IOError)

# "Wait 2^x * 10000 milliseconds between each retry"
@retry(retry_on_exception=retry_if_asset_ok,
       wait_exponential_multiplier=10000)
def download_item(session, item_id, item_dest, asset_type, item_type):
    print(f"download: \t {item_id}")
    
    # request an item
    item = session.get(("https://api.planet.com/data/v1/item-types/" +
                        "{}/items/{}/assets/").format(item_type, item_id))
    
    # raise an exception to trigger the retry
    if item.status_code != 200:
        print(item)
        raise IOException("Cannot get item activate link")
    # check if item is ready (active)
    if item.json()[asset_type]['status'] != 'active':
        print(f"download: \t {item_id} is {item.json()[asset_type]['status']}")
        raise Exception("Item is not active yet")
    
    # download image to the given destination
    download_cmd = item.json()[asset_type]['location']
    urllib.request.urlretrieve(download_cmd, item_dest)
    print(f"download: \t {item_id}   finished ... ")
    return item.json()[asset_type]['status']


# "Wait 2^x * 10000 milliseconds between each retry"
@retry(retry_on_exception=retry_if_asset_ok,
       wait_exponential_multiplier=10000)
def activate_item(session, item_id, asset_type, item_type):
    """ activate item """
    print(f"activate: \t {item_id}")
    
    # request an item
    item = session.get(("https://api.planet.com/data/v1/item-types/" +
                        "{}/items/{}/assets/").format(item_type, item_id))
    
    # raise an exception to trigger the retry
    if item.status_code != 200:
        raise IOException(f"Cannot get item activate link - {item}")
    
    # request activation
    response = session.post(item.json()[asset_type]["_links"]["activate"])
    
    if response.status_code == EXCEED_QUOTA:
        print(f"activate: \t {item_id}   rate limit exceeded")
        raise Exception("Rate limit error at running activate command")
    elif response.status_code != 202 or response.status_code != 204:
        raise Exception("Unknown error, try again")
    
    print(f"activate: \t {item_id}   success ... with code {response.status_code}")
    return response.status_code

def activate_download_item(session, item_id, item_dest, asset_type="visual", item_type="PSOrthoTile"):
    if os.path.exists(item_dest):
        print(f"already have: \t {item_id}")
        return 'already have'
    activate_item(session, item_id, asset_type, item_type)
    download_item(session, item_id, item_dest, asset_type, item_type)

    
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