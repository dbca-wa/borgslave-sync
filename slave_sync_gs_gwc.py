import json
import logging
import traceback
import requests

from gwc import GeoWebCache

from slave_sync_env import (
    GEOSERVER_URL,GEOSERVER_USERNAME,GEOSERVER_PASSWORD
)

from slave_sync_task import (
    update_wmslayer_job,update_layergroup_job,update_feature_job,update_feature_metadata_job,gs_task_filter,gs_feature_task_filter,gs_spatial_task_filter,
    empty_gwc_layer_job,empty_gwc_group_job,
)

logger = logging.getLogger(__name__)

gwc = GeoWebCache(GEOSERVER_URL,GEOSERVER_USERNAME,GEOSERVER_PASSWORD)

update_headers = {'content-type':'application/xml','Accept': 'application/xml'}

task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"])

def update_gwc(sync_job,task_metadata,task_status):
    """
    update a gwc
    """
    #create the cached layer
    if "geoserver_setting" in sync_job and sync_job["geoserver_setting"].get("create_cache_layer",False):
        #need to create cache layer
        gwc.update_layer(sync_job)
        task_status.set_message("message","Update gwc successfully")
    else:
        if gwc.get_layer(sync_job['workspace'], sync_job['name']):
            gwc.del_layer(sync_job['workspace'],sync_job['name'])
            task_status.set_message("message","Remove gwc successfully")
        else:
            task_status.set_message("message","Nothing happened.")



def empty_gwc(sync_job,task_metadata,task_status):
    """
    empty gwc
    """
    if gwc.get_layer(sync_job['workspace'], sync_job['name']):
        gwc.empty_layer(sync_job)
        task_status.set_message("message","Empty gwc successfully")
    else:
        task_status.set_message("message","GWC is disabled, no need to empty gwc.")


tasks_metadata = {
                ("update_gwc", update_wmslayer_job  , gs_task_filter        , task_name, update_gwc),
                ("update_gwc", update_layergroup_job, gs_task_filter        , task_name, update_gwc),
                ("update_gwc", update_feature_job   , gs_spatial_task_filter, task_name, update_gwc),
                ("update_gwc", update_feature_metadata_job   , gs_spatial_task_filter, task_name, update_gwc),
                ("empty_gwc", empty_gwc_layer_job  , gs_task_filter        , task_name, empty_gwc),
                ("empty_gwc", empty_gwc_group_job  , gs_task_filter        , task_name, empty_gwc),
}

