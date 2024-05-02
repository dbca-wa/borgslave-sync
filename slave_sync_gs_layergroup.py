import json
import logging
import requests


import slave_sync_env as settings
from . import geoserver_restapi as gs

from slave_sync_task import (
    update_layergroup_job,remove_layergroup_job,gs_task_filter,
    get_http_response_exception,get_task,execute_task
)

logger = logging.getLogger(__name__)

update_headers = {'content-type':'application/xml','Accept': 'application/xml'}

task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"])

def _update_group(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    update a layer group
    """
    #update the dependent group first
    sub_task = None
    for group in sync_job.get('dependent_groups',{}):
        sub_task = get_task("update_layergroup",task_name(group))
        if sub_task:
            #dependent task exist,execute it.
            execute_task(*sub_task)
            if sub_task[0]["status"].is_not_succeed:
                #sub task failed, 
                raise Exception("update sub group ({0}) failed.".format(sub_task[0]["name"]))

    workspace = sync_job['workspace']
    groupname = sync_job['name'])
    extra_data = {}
    if sync_job.get('keywords')  and sync_job.get('applications'):
        keywords = sync_job["keywords"] + sync_job["applications"]
    elif sync_job.get('keywords') :
        keywords = sync_job["keywords"]
    elif sync_job.get('applications'):
        keywords = sync_job["applications"]
    else:
        keywords = []

    extra_data["keywords"] = keywords

    gs.update_layergroup(geoserver_url,username,password,workspace,groupname,collections.ChainMap(extra_data,sync_job))

def update_group(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_group)

def _remove_group(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    remove a layer group
    """
    workspace = sync_job['workspace']
    groupname = sync_job['name'])
    gs.delete_layergroup(geoserver_url,username,password,workspace,groupname)

def remove_group(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_remove_group)


tasks_metadata = {
                ("update_layergroup", update_layergroup_job, gs_task_filter, task_name, update_group),
                ("remove_layergroup", remove_layergroup_job, gs_task_filter, task_name, remove_group)
}

