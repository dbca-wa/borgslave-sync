import json
import logging
import requests


from slave_sync_env import (
    GEOSERVER_REST_URL,GEOSERVER_USERNAME, GEOSERVER_PASSWORD,
    template_env
)
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

def get_stores_url(workspace):
    """
    return the url of the wms stores
    """
    return "{0}workspaces/{1}/wmsstores.xml".format(GEOSERVER_REST_URL,workspace)

def get_store_url(workspace,store_name):
    """
    return the url of the wms store
    """
    return "{0}workspaces/{1}/wmsstores/{2}.xml".format(GEOSERVER_REST_URL,workspace,store_name)

def get_layers_url(workspace,store_name):
    """
    return the url of the wms store layers
    """
    return "{0}workspaces/{1}/wmsstores/{2}/wmslayers.xml".format(GEOSERVER_REST_URL,workspace,store_name)

def get_layer_url(workspace,store_name,layer_name):
    """
    return the url of the wms layer
    """
    return "{0}workspaces/{1}/wmsstores/{2}/wmslayers/{3}.xml?recurse=true".format(GEOSERVER_REST_URL,workspace,store_name,layer_name)

def update_store(sync_job,task_metadata,task_status):
    """
    update a store
    """
    if sync_job.get('geoserver_setting',None) is None:
        sync_job['geoserver_setting'] = {}

    res = requests.get(get_store_url(sync_job['workspace'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    if res.status_code == 200:
        http_method = requests.put
        request_url = get_store_url(sync_job['workspace'],sync_job['name'])
    else:
        http_method = requests.post
        request_url = get_stores_url(sync_job['workspace'])

    template = template_env.get_template('wms_store.xml')
    res = http_method(request_url, auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD), headers=update_headers, data=template.render(sync_job))
      
    if res.status_code >= 400:
        raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

def update_layer(sync_job,task_metadata,task_status):
    """
    update a layer
    """
    sync_job['applications'] = sync_job['applications'] or []
    sync_job['keywords'] = sync_job['keywords'] or []
    res = requests.get(get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    if res.status_code == 200:
        http_method = requests.put
        request_url = get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name'])
    else:
        http_method = requests.post
        request_url = get_layers_url(sync_job['workspace'],sync_job['store'])

    template = template_env.get_template('wms_layer.xml')
    res = http_method(request_url, auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD), headers=update_headers, data=template.render(sync_job))
  
    if res.status_code >= 400:
        raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

def remove_layer(sync_job,task_metadata,task_status):
    """
    remove a layer
    """
    res = requests.get(get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    if res.status_code == 200:
        #store exist,delete it
        res = requests.delete(get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
      
        if res.status_code >= 400:
            raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

def remove_store(sync_job,task_metadata,task_status):
    """
    remove a store
    """
    logger.info("Begin to remove store ({}).".format(sync_job['name']))
    res = requests.get(get_store_url(sync_job['workspace'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    if res.status_code == 200:
        #store exist,delete it
        res = requests.delete(get_store_url(sync_job['workspace'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
      
        if res.status_code >= 400:
            raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))

tasks_metadata = [
                    ("update_wmsstore", update_wmsstore_job, gs_task_filter, task_name, update_store),
                    ("update_wmslayer", update_wmslayer_job, gs_task_filter, task_name, update_layer),
                    ("remove_wmslayer", remove_wmslayer_job, gs_task_filter, task_name, remove_layer),
                    ("remove_wmsstore", remove_wmsstore_job, gs_task_filter, task_name, remove_store)
]

