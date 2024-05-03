import json
import logging
import collections
import requests

import slave_sync_env as settings
import geoserver_restapi as gs
from slave_sync_task import (
    update_wmsstore_job,update_wmslayer_job,remove_wmslayer_job,remove_wmsstore_job,gs_task_filter,
    get_http_response_exception
)

logger = logging.getLogger(__name__)

update_headers = {'content-type':'application/xml','Accept': 'application/xml'}

task_name = lambda t: "{0}:{1}".format(t["workspace"],t["name"])

def get_store_name(workspace,store_name):
    """
    return the name of the wms store
    """
    return store_name

def _update_store(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    update a store
    """
    workspace = sync_job['workspace']
    storename = sync_job['name']

    if "geoserver_setting" in sync_job:
        gs.update_wmsstore(geoserver_url,username,password,workspace,storename,collections.ChainMap(sync_job["geoserver_setting"],sync_job))
    else:
        gs.update_wmsstore(geoserver_url,username,password,workspace,storename,sync_job)

def update_store(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_store)


def _remove_store(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    remove a store
    """
    workspace = sync_job['workspace']
    storename = sync_job['name']
    gs.delete_wmsstore(geoserver_url,username,password,workspace,storename)

def remove_store(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_remove_store)

def _update_layer(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    update a layer
    """
    workspace = sync_job['workspace']
    storename = sync_job['store']
    layername = sync_job['name']

    extra_dict = {}

    if sync_job.get('keywords')  and sync_job.get('applications'):
        keywords = sync_job["keywords"] + sync_job["applications"]
    elif sync_job.get('keywords') :
        keywords = sync_job["keywords"]
    elif sync_job.get('applications'):
        keywords = sync_job["applications"]
    else:
        keywords = None
    extra_dict["keywords"] = keywords

    if (sync_job.get('override_bbox',False)):
        bbox = sync_job["bbox"]
        bbox = (repr(bbox[0]),repr(bbox[2]),repr(bbox[1]),repr(bbox[3]),sync_job["crs"])
        extra_data["nativeBoundingBox"] = bbox
        extra_data["latLonBoundingBox"] = bbox
        extra_data["nativeCRS"] = sync_job("crs")
        extra_data["srs"] = sync_job("crs")
    else:
        extra_data["nativeBoundingBox"] = None
        extra_data["latLonBoundingBox"] = None
        extra_data["nativeCRS"] = None
        extra_data["srs"] = None

    gs.update_wmslayer(geoserver_url,username,password,workspace,storename,layername,collections.ChainMap(extra_dict,sync_job))

def update_layer(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_layer)

def _remove_layer(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    remove a layer
    """
    workspace = sync_job['workspace']
    layername = sync_job['name']
    gs.delete_wmslayer(geoserver_url,username,password,workspace,layername)

def remove_layer(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_remove_layer)

tasks_metadata = [
                    ("update_wmsstore", update_wmsstore_job, gs_task_filter, task_name, update_store),
                    ("update_wmslayer", update_wmslayer_job, gs_task_filter, task_name, update_layer),
                    ("remove_wmslayer", remove_wmslayer_job, gs_task_filter, task_name, remove_layer),
                    ("remove_wmsstore", remove_wmsstore_job, gs_task_filter, task_name, remove_store)
]

