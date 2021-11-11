import json
import logging
import traceback
import requests

from gwc import GeoWebCache

import slave_sync_env as settings

from slave_sync_task import (
    update_wmslayer_job,update_layergroup_job,update_feature_job,update_feature_metadata_job,gs_task_filter,gs_feature_task_filter,gs_spatial_task_filter,
    empty_gwc_layer_job,empty_gwc_group_job,empty_gwc_feature_job,
    empty_gwc_livelayer_job,update_livelayer_job
)

logger = logging.getLogger(__name__)

update_headers = {'content-type':'application/xml','Accept': 'application/xml'}

task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"])

def _update_gwc(sync_job,task_metadata,task_status,gwc,stage=None):
    """
    update a gwc
    """
    #create the cached layer
    if "geoserver_setting" in sync_job and sync_job["geoserver_setting"].get("create_cache_layer",False):
        #need to create cache layer
        gwc.update_layer(sync_job)
        task_status.set_message("message","Update gwc successfully",stage=stage)
    else:
        if gwc.get_layer(sync_job['workspace'], sync_job['name']):
            gwc.del_layer(sync_job['workspace'],sync_job['name'])
            task_status.set_message("message","Remove gwc successfully",stage=stage)
        else:
            task_status.set_message("message","Nothing happened.",stage=stage)

def update_gwc(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_gwc,lambda index:(GeoWebCache(settings.GEOSERVER_URL[index],settings.GEOSERVER_USERNAME[index],settings.GEOSERVER_PASSWORD[index]),))


def _empty_gwc(sync_job,task_metadata,task_status,gwc,stage=None):
    """
    empty gwc
    """
    if gwc.get_layer(sync_job['workspace'], sync_job['name']):
        gwc.empty_layer(sync_job)
        task_status.set_message("message","Empty gwc successfully",stage=stage)
    else:
        task_status.set_message("message","GWC is disabled, no need to empty gwc.",stage=stage)

def empty_gwc(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_empty_gwc,lambda index:(GeoWebCache(settings.GEOSERVER_URL[index],settings.GEOSERVER_USERNAME[index],settings.GEOSERVER_PASSWORD[index]),))


tasks_metadata = {
                ("update_gwc", update_wmslayer_job  , gs_task_filter        , task_name, update_gwc),
                ("update_gwc", update_layergroup_job, gs_task_filter        , task_name, update_gwc),
                ("update_gwc", update_feature_job   , gs_spatial_task_filter, task_name, update_gwc),
                ("update_gwc", update_livelayer_job   , gs_spatial_task_filter, task_name, update_gwc),
                ("update_gwc", update_feature_metadata_job   , gs_spatial_task_filter, task_name, update_gwc),
                ("empty_gwc", empty_gwc_layer_job  , gs_task_filter        , task_name, empty_gwc),
                ("empty_gwc", empty_gwc_group_job  , gs_task_filter        , task_name, empty_gwc),
                ("empty_gwc", empty_gwc_feature_job  , gs_task_filter        , task_name, empty_gwc),
                ("empty_gwc", empty_gwc_livelayer_job  , gs_task_filter        , task_name, empty_gwc),
}

