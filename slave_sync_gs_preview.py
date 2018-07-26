import logging
import traceback
import math
import os
import requests
import json

import geoserver_catalog_extension
from slave_sync_env import (
    PREVIEW_ROOT_PATH,SLAVE_NAME,
    GEOSERVER_URL,GEOSERVER_USERNAME,GEOSERVER_PASSWORD,
)

from slave_sync_task import (
    update_feature_job,gs_spatial_task_filter,update_livelayer_job,layer_preview_task_filter,update_wmslayer_job
)

logger = logging.getLogger(__name__)

australia_bbox = (108.0000,-45.0000,155.0000,-10.0000)
preview_dimension = (480,278)

def get_preview_dimension(bbox):
    if isinstance(bbox,basestring):
        bbox = json.loads(bbox)
    bbox_ratio = [(bbox[2] - bbox[0]) / (australia_bbox[2] - australia_bbox[0]),(bbox[3] - bbox[1]) / (australia_bbox[3] - australia_bbox[1])]
    if (bbox_ratio[0] > bbox_ratio[1]):
        return (preview_dimension[0],int(math.floor(preview_dimension[1] * (bbox_ratio[1] / bbox_ratio[0]))))
    else:
        return (int(math.floor(preview_dimension[0] * bbox_ratio[0] * (1 / bbox_ratio[1]))),preview_dimension[1])
    


task_layer_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

base_preview_url = GEOSERVER_URL + "/ows?service=wms&version=1.1.0&request=GetMap&srs=EPSG%3A4326&format=image/png"

def layer_preview_urls(sync_job):
    urls = []
    height = None
    link = None

    #get GWC supported url
    if "ows_resource" in sync_job:
        for resource in sync_job["ows_resource"]:
            if resource["protocol"] == "OGC:WMS" and resource["link"]:
                height = int(resource.get("height") or "0")
                link = resource["link"]
                #replace the kmi url with local url
                link = GEOSERVER_URL + link[link.find("/geoserver") + len("/geoserver"):]
                if resource["format"] == "image/jpeg":
                    urls.append((link,".jpg",height))
                else:
                    urls.append((link,".{}".format(resource["format"][6:]),height))
        urls.sort(key=lambda data:data[2])
    #add GetMap url 
    bbox = sync_job.get("bbox") or australia_bbox
    dimension = get_preview_dimension(bbox)
    preview_url = base_preview_url  + "&bbox=" + ",".join([str(d) for d in bbox]) + "&layers=" + "%3A".join([sync_job["workspace"],sync_job['name']]) + "&width=" + str(dimension[0]) + "&height=" + str(dimension[1])
    
    urls.append( (preview_url,".png",-1))

    return urls

def get_layer_preview(sync_job,task_metadata,task_status):
    urls = layer_preview_urls(sync_job)

    logger.info("Try to get layer preview image")
    index = 0
    for url in urls:
        index += 1
        resp = requests.get(url[0], auth=(GEOSERVER_USERNAME,GEOSERVER_PASSWORD))
        if resp.status_code >= 400:
            if index == len(urls):
                raise Exception("Get layer's preview image failed. ({0}: {1})".format(resp.status_code, resp.content))
            else:
                continue
        elif not resp.headers['Content-Type'].startswith("image/"):
            if resp.text.find("This request used more time than allowed") >= 0:
                if index == len(urls):
                    #GWC disabled getmap request
                    #timeout when try to get preview image
                    task_status.task_failed()
                    task_status.del_message("preview_file")
                    task_status.set_message("message",resp.text)
                else:
                    #GWC enabled getmap request
                    #this time timeout; next time, has high change to succeed becuare the map is prepared by before request
                    raise Exception("Get layer's preview image timeout.")
            else:
                if index == len(urls):
                    raise Exception("url:{}\r\n{}".format(url,resp.text))
                else:
                    continue
        #getmap succeed
        try:
            folder = os.path.join(PREVIEW_ROOT_PATH,SLAVE_NAME,sync_job["channel"],sync_job["workspace"])
            if not os.path.exists(folder):   os.makedirs(folder)
            filename = os.path.join(folder,sync_job["name"] + url[1])

            with open(filename, 'wb') as fd:
                for chunk in resp.iter_content(1024):
                    fd.write(chunk)

            task_status.set_message("preview_file",filename[len(PREVIEW_ROOT_PATH) + 1:])
            break
        except:
            raise Exception("Get layer's preview image failed. {0}".format(resp.content))


tasks_metadata = [
                    ("get_layer_preview", update_feature_job, layer_preview_task_filter      , task_layer_name  , get_layer_preview),
                    ("get_layer_preview", update_livelayer_job, layer_preview_task_filter      , task_layer_name  , get_layer_preview),
                    ("get_layer_preview", update_wmslayer_job, layer_preview_task_filter      , task_layer_name  , get_layer_preview),
]

