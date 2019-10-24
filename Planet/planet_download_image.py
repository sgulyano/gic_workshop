#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 15:58:58 2019

@author: yoyo
"""

import os
import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from requests.auth import HTTPBasicAuth
from multiprocessing.dummy import Pool as ThreadPool

from planet_utils import get_datetime, get_save_search, activate_download_item, load_img_ids, save_img_ids
from pathum_filter import adm1_pathum_bbox_df as pathum_geometry
from pathum_filter import get_pathum_filter, get_stats, get_ids

## User Params
img_folder = './data'
img_id_file = 'pathum_2019-01-01_2019-02-01.txt'

#%% get ids of images using saved search, 
search_id, img_ids = load_img_ids(img_id_file)

with open(img_id_file, 'r') as f:
    search_id = f.readline().strip()
    item_type = f.readline().strip()
    asset_type = f.readline().strip()
    img_ids = [i.rstrip() for i in f.readlines()]

print('Search ID  = %s' % search_id)
print('Item Type  = %s' % item_type)
print('Asset Type = %s' % asset_type)

#%% get destination of all images
df = pd.DataFrame({'id':img_ids, 
                   'year' : [im_id[16:20] for im_id in img_ids], 
                   'month': [im_id[21:23] for im_id in img_ids], 
                   'date' : [im_id[16:26] for im_id in img_ids]})

def get_dest(r):
    return os.path.join(img_folder, 
                        item_type, 
                        asset_type, 
                        r.year, 
                        r.month, 
                        r.date, 
                        r.id + '.tiff')

df['dest'] = df.apply(get_dest, axis=1)
print(df.head())

#%% create dir for all files
for item_dest in df['dest']:
    directory = os.path.dirname(item_dest)
    if not os.path.exists(directory):
        os.makedirs(directory)

#%% activate and download all images
# setup auth
print('Start download satellite images to   %s ...' % img_folder)
session = requests.Session()
session.auth = (os.environ['PL_API_KEY'], '')

def act_dl_itm(item_id, item_dest):
    activate_download_item(session, item_id, item_dest, asset_type="visual", item_type="PSOrthoTile")

# An easy way to parallise I/O bound operations in Python
# is to use a ThreadPool.
parallelism = 8
thread_pool = ThreadPool(parallelism)
thread_pool.starmap(act_dl_itm, df[['id','dest']].itertuples(index=False))