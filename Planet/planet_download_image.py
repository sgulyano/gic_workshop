#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 15:58:58 2019

@author: yoyo
"""

import os
import numpy as np
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from multiprocessing.dummy import Pool as ThreadPool
import urllib.request
from retrying import retry

from planet_utils import get_dest_df, read_img_id_file

EXCEED_QUOTA = 403 # used to be 429 from changelog May 2019
EXCEED_RATE = 429
MAX_ATTEMPT = 3
WAIT_S = 10
    

def retry_if_asset_ok(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return not isinstance(exception, IOError)


# "Wait 2^x * 10000 milliseconds between each retry"
@retry(retry_on_exception=retry_if_asset_ok,
       stop_max_attempt_number=MAX_ATTEMPT,
       wait_exponential_multiplier=WAIT_S*1000)
def download_item(session, item_id, item_dest, asset_type, item_type):
    # request an item
    item = session.get(("https://api.planet.com/data/v1/item-types/" +
                        "{}/items/{}/assets/").format(item_type, item_id))
    
    # raise an exception to trigger the retry
    if item.status_code != 200:
        raise IOError(f"Cannot get item activate link - {item}")
    # check if item is ready (active)
    if item.json()[asset_type]['status'] != 'active':
        print(f"download: \t {item_id} is {item.json()[asset_type]['status']}")
        raise Exception(f"download: \t {item_id}   failed ... ")
    
    # download image to the given destination
    download_cmd = item.json()[asset_type]['location']
    
    try:
        urllib.request.urlretrieve(download_cmd, item_dest)
    except urllib.error.HTTPError:
        print(f"download: \t {item_id}   failed ... HTTPError")
        raise Exception(f"download: \t {item_id}   failed ... HTTPError")
    except:
        print(f"download: \t {item_id}   failed ... Error")
        raise Exception(f"download: \t {item_id}   failed ... Error")
    
    print(f"download: \t {item_id}   completed ... ")
    return item.json()[asset_type]['status']


# "Wait 2^x * 10000 milliseconds between each retry"
@retry(retry_on_exception=retry_if_asset_ok,
       stop_max_attempt_number=MAX_ATTEMPT,
       wait_exponential_multiplier=WAIT_S*1000)
def activate_item(session, item_id, asset_type, item_type):
    """ activate item """
    # request an item
    item = session.get(("https://api.planet.com/data/v1/item-types/" +
                        "{}/items/{}/assets/").format(item_type, item_id))
    
    # raise an exception to trigger the retry
    if item.status_code != 200:
        raise IOError(f"Cannot get item activate link - {item}")
    
    # request activation
    response = session.post(item.json()[asset_type]["_links"]["activate"])
    
    if response.status_code == EXCEED_QUOTA:
        raise IOError(f"activate: \t {item_id}   quota exceeded")
    elif response.status_code == EXCEED_RATE:
        print(f"activate: \t {item_id}   retrying")
        raise Exception(f"activate: \t {item_id}   rate limit exceeded")
    elif response.status_code != 202 and response.status_code != 204:
        raise IOError(f"activate: \t {item_id}   unknown code {response.status_code}")
    
    print(f"activate: \t {item_id}   success ... with code {response.status_code}")
    return response.status_code


def activate_download_item(session, item_id, item_dest, asset_type="visual", item_type="PSOrthoTile"):
    if os.path.exists(item_dest):
        print(f"already have: \t {item_id}")
        return 'already have'
    print(f"activate: \t {item_id}")
    try:
        activate_item(session, item_id, asset_type, item_type)
    except Exception as e: 
        print(e)
        if os.path.isfile(item_dest):
            os.remove(item_dest)
            print('file removed: \t {item_id}')
        return
    print(f"download: \t {item_id}")
    try:
        download_item(session, item_id, item_dest, asset_type, item_type)
    except Exception as e: 
        print(e)
        if os.path.isfile(item_dest):
            os.remove(item_dest)
            print('file removed: \t {item_id}')
        return

        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("img_id_file", help="Planet satellite image ID file")
    parser.add_argument("-o", "--output", default='./data', help="Output folder for storing satellite images")
    args = parser.parse_args()
    
    assert(os.path.isfile(args.img_id_file))
    assert(os.path.isdir(args.output))
    
    search_id, item_type, asset_type, img_ids = read_img_id_file(args.img_id_file)
    print('Search ID  = %s' % search_id)
    print('Item Type  = %s' % item_type)
    print('Asset Type = %s' % asset_type)

    # get destination of all images
    df = get_dest_df(img_ids, args.output, item_type, asset_type)
    print(df.head())
    
    #%% create dir for all files
    for item_dest in df['dest']:
        directory = os.path.dirname(item_dest)
        if not os.path.exists(directory):
            os.makedirs(directory)

    #%% activate and download all images
    # setup auth
    print('Start download satellite images to   %s ...' % args.output)
    session = requests.Session()
    session.auth = (os.environ['PL_API_KEY'], '')

    def act_dl_itm(item_id, item_dest):
        activate_download_item(session, item_id, item_dest, asset_type="visual", item_type="PSOrthoTile")

    # An easy way to parallise I/O bound operations in Python
    # is to use a ThreadPool.
    parallelism = 8
    thread_pool = ThreadPool(parallelism)
    thread_pool.starmap(act_dl_itm, df[['id','dest']].itertuples(index=False))