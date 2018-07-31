import json
import logging
import re
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

def get_store_url(workspace,store_name,f='xml'):
    """
    return the url of the wms store
    """
    return "{0}workspaces/{1}/wmsstores/{2}.{3}".format(GEOSERVER_REST_URL,workspace,store_name,f)

def get_wmsstores_url(workspace,f="xml"):
    """
    return the url of the wms store layers
    """
    return "{0}workspaces/{1}/wmsstores.{2}".format(GEOSERVER_REST_URL,workspace,f)

def get_layers_url(workspace,store_name,f='xml'):
    """
    return the url of the wms store layers
    """
    return "{0}workspaces/{1}/wmsstores/{2}/wmslayers.{3}".format(GEOSERVER_REST_URL,workspace,store_name,f)

def get_layer_url(workspace,store_name,layer_name,f='xml'):
    """
    return the url of the wms layer
    """
    return "{0}workspaces/{1}/wmsstores/{2}/wmslayers/{3}.{4}?recurse=true".format(GEOSERVER_REST_URL,workspace,store_name,layer_name,f)

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

re_laye_not_in_capabilitiles_doc = re.compile("Could\snot\sfind\slayer.+in\sthe\sserver\scapabilitiles\sdocument",re.IGNORECASE|re.DOTALL)
re_already_exist_in_workspace = re.compile("Resource\snamed.+already\sexists\sin\snamespace.+",re.IGNORECASE|re.DOTALL)
def update_layer(sync_job,task_metadata,task_status):
    """
    update a layer
    """
    sync_job['applications'] = sync_job.get('applications') or []
    sync_job['keywords'] = sync_job.get('keywords') or []
    if (sync_job.get('override_bbox',False)):
       #sync_job["bbox"] = json.loads(sync_job["bbox"])
       pass
    
    template = template_env.get_template('wms_layer.xml')
    xmlData = template.render(sync_job).encode("utf-8")

    failover = True
    while True:
        res = requests.get(get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
        if res.status_code == 200:
            http_method = requests.put
            request_url = get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name'])
        else:
            http_method = requests.post
            request_url = get_layers_url(sync_job['workspace'],sync_job['store'])
    
        res = http_method(request_url, auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD), headers=update_headers, data=xmlData)
      
        if res.status_code >= 400:
            errMsg = get_http_response_exception(res)
            if failover and re_already_exist_in_workspace.search(errMsg):
                failover = False
                #check whether layer exist or not, if exist, delete it.
                res1 = requests.get(get_wmsstores_url(sync_job['workspace'],'json'), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
                if res1.status_code == 200:
                    isExist = False
                    for store in res1.json().get("wmsStores",{}).get("wmsStore",[]):
                        res2 = requests.get(get_layer_url(sync_job['workspace'],store["name"],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
                        if res2.status_code == 200:
                            #layer exist,delete it
                            isExist = True
                            res3 = requests.delete(get_layer_url(sync_job['workspace'],store['name'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
                            if res3.status_code >= 400:
                                raise Exception("{0}: {1}".format(res3.status_code, get_http_response_exception(res3)))
                            break
                    if isExist:
                        continue
            raise Exception("{0}: {1}".format(res.status_code, get_http_response_exception(res)))
        else:
            break

def remove_layer(sync_job,task_metadata,task_status):
    """
    remove a layer
    """
    res = requests.get(get_layer_url(sync_job['workspace'],sync_job['store'],sync_job['name']), auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD))
    if res.status_code == 200:
        #layer exist,delete it
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

