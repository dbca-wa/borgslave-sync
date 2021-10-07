#!/usr/bin/env python

import requests
import json
import logging
import re
import os, sys
import itertools
import pdb
import xmltodict


from slave_sync_task import (
    update_feature_job,update_feature_metadata_job,gs_feature_task_filter,remove_feature_job,
    update_wmslayer_job,remove_wmslayer_job,gs_task_filter
)
from slave_sync_env import (GEOSERVER_URL,GEOSERVER_REST_URL,GEOSERVER_USERNAME,GEOSERVER_PASSWORD,GEOSERVER_THEME_DIR,GEOSERVER_DATA_DIR,GEOSERVER_WMS_GETCAPABILITIES_URL)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s %(levelname)s %(message)s',
)

def update_static_catalogues(task,task_metadata,task_status):
    r = requests.get(GEOSERVER_WMS_GETCAPABILITIES_URL, auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    # this code makes assumptions about how the catalog XML will be formatted! use caution!

    # filter out all of the complete <Layer></Layer> blocks that describe one layer
    # (GeoServer nests these inside another parent <Layer> block for some reason, ignore that)
    rx = re.compile("(<Layer .*?>.*?</Layer>)", re.MULTILINE|re.DOTALL)
    layers = rx.findall(r.content)

    layer_map = {}
    app_layers = {}
    
    if len(layers) == 0:
        raise Exception(r.content)

    for l in layers:
        # read the XML layer blob into a dictionary, so it can be converted to JSON
        # FIXME: default format from xmltodict is pretty lousy, need to do a better job
        # at scrubbing the data 
        l_d = xmltodict.parse(l)
        tree = l_d["Layer"]
        name = tree["Name"]

        record = {
            "xml": l,
            "tree": tree
        }
        # check for keywords
        if "KeywordList" in tree and tree["KeywordList"]:
            keyword_list = tree["KeywordList"].get("Keyword", [])
            if type(keyword_list) != list:
                keyword_list = [keyword_list]
            tree["KeywordList"] = keyword_list
            for k in keyword_list:
                # we're looking for keywords in the format [app_name]:[order]
                if ":" in k:
                    logger.info("Found application reference in keywords for {}: {}".format(name, k))
                    app, order = k.split(":", 1)
                    if app not in app_layers:
                        app_layers[app] = []
                    app_layers[app].append((int(order), name))
                
        layer_map[name] = record
   

    # find the position in the XML where the child layer blocks start
    layers_start = r.content.find("<Layer", r.content.find("<Layer")+1)
    # find the position in the XML where they end
    layers_end = r.content.rfind("</Layer>", 0, r.content.rfind("</Layer>")-1)+8

    # create path for storing static files if doesn't exist
    if not os.path.isdir(GEOSERVER_THEME_DIR):
        os.makedirs(GEOSERVER_THEME_DIR)

    # compare what we're going to write with what's already there, clear out unwanted files
    files = os.listdir(GEOSERVER_THEME_DIR)
    targets = list(itertools.chain(*[('{}.wms'.format(a), '{}.json'.format(a)) for a in app_layers ]))
    for f in files:
        if f not in targets:
           os.remove(os.path.join(GEOSERVER_THEME_DIR, f))

    # write catalogues to output
    for app, layers in app_layers.items():
        layers.sort()
        xml_out = r.content[:layers_start] + "".join([layer_map[l[1]]["xml"] for l in layers]) + r.content[layers_end:]
        json_out = json.dumps([layer_map[l[1]]["tree"] for l in layers], indent=4)
       
        logger.info("Outputting {0}.wms/{0}.json with {1} layers".format(app, len(layers)))

        with open(os.path.join(GEOSERVER_THEME_DIR, "{}.wms".format(app)), "wb") as f:
            f.write(xml_out)

        with open(os.path.join(GEOSERVER_THEME_DIR, "{}.json".format(app)), "wb") as f:
            f.write(json_out)

tasks_metadata = [
                    ("update_catalogues", update_feature_job, gs_feature_task_filter, "update_catalogues"  , update_static_catalogues),
                    ("update_catalogues", update_feature_metadata_job, gs_feature_task_filter, "update_catalogues"  , update_static_catalogues),
                    ("update_catalogues", remove_feature_job, gs_feature_task_filter, "update_catalogues"  , update_static_catalogues),
                    ("update_catalogues", update_wmslayer_job, gs_task_filter        , "update_catalogues"  , update_static_catalogues),
                    ("update_catalogues", remove_wmslayer_job, gs_task_filter        , "update_catalogues"  , update_static_catalogues),
]   
if __name__ == '__main__':
    update_static_catalogues()
