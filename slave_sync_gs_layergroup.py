import json
import logging
import requests


import slave_sync_env as settings

from slave_sync_task import (
    update_layergroup_job,remove_layergroup_job,gs_task_filter,
    get_http_response_exception,get_task
)

logger = logging.getLogger(__name__)

update_headers = {'content-type':'application/xml','Accept': 'application/xml'}

task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"])

def get_groups_url(rest_url,workspace):
    """
    return the url of layer groups
    """
    return "{0}/workspaces/{1}/layergroups.xml".format(rest_url,workspace)

def get_group_url(rest_url,workspace,group_name):
    """
    return the url of the layer group
    """
    return "{0}/workspaces/{1}/layergroups/{2}.xml".format(rest_url,workspace,group_name)

def _update_group(sync_job,task_metadata,task_status,rest_url,username,password,stage=None):
    """
    update a layer group
    """
    #update the dependent group first
    sub_task = None
    for group in sync_job.get('dependent_groups',{}):
        sub_task = get_task("update_layer_group",task_name(group))
        if sub_task:
            #dependent task exist,execute it.
            execute_task(*sub_task)
            if sub_task[0]["status"].is_not_succeed:
                #sub task failed, 
                raise Exception("update sub group ({0}) failed.".format(sub_task[0]["name"]))

    #after update all dependent groups, begin to update current group
    res = requests.get(get_group_url(rest_url,sync_job['workspace'],sync_job['name']), auth=(username,password))
    if res.status_code == 200:
        http_method = requests.put
        request_url = get_group_url(rest_url,sync_job['workspace'],sync_job['name'])
    else:
        http_method = requests.post
        request_url = get_groups_url(rest_url,sync_job['workspace'])

    template = settings.template_env.get_template('layergroup.xml')
    res = http_method(request_url, auth=(username,password), headers=update_headers, data=template.render(sync_job))
    if res.status_code >= 400:
        if http_method == requests.put:
            logger.warning("update group({0}) failed, try to delete and readd it".format(sync_job['name']))
            #update layergroup with different number of layers will cause "Layer group has different number of styles than layers"
            #so delete layergroup first and then readd it.
            res = requests.delete(get_group_url(rest_url,sync_job['workspace'],sync_job['name']), auth=(username,password))
            if res.status_code >= 400:
                raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))
            http_method = requests.post
            request_url = get_groups_url(rest_url,sync_job['workspace'])
            res = http_method(request_url, auth=(username,password), headers=update_headers, data=template.render(sync_job))
            if res.status_code >= 400:
                raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))
        else:
            raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

def update_group(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_group,lambda index:(settings.GEOSERVER_REST_URL[index],settings.GEOSERVER_USERNAME[index],settings.GEOSERVER_PASSWORD[index]))


def _remove_group(sync_job,task_metadata,task_status,rest_url,username,password,stage=None):
    """
    remove a layer group
    """
    logger.info("Begin to remove layer group ({}).".format(sync_job['name']))
    res = requests.get(get_group_url(rest_url,sync_job['workspace'],sync_job['name']), auth=(username,password))
    if res.status_code == 200:
        #store exist,delete it
        res = requests.delete(get_group_url(rest_url,sync_job['workspace'],sync_job['name']), auth=(username,password))
      
        if res.status_code >= 400:
            raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

def remove_group(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_remove_group,lambda index:(settings.GEOSERVER_REST_URL[index],settings.GEOSERVER_USERNAME[index],settings.GEOSERVER_PASSWORD[index]))


tasks_metadata = {
                ("update_layergroup", update_layergroup_job, gs_task_filter, task_name, update_group),
                ("remove_layergroup", remove_layergroup_job, gs_task_filter, task_name, remove_group)
}

