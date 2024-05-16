import logging
import traceback
import math
import os
import sys
import requests
import json

import slave_sync_env as settings

from slave_sync_task import (
    update_feature_job,gs_spatial_task_filter,update_livelayer_job,layer_preview_task_filter,update_wmslayer_job
)

logger = logging.getLogger(__name__)

australia_bbox = (108.0000,-45.0000,155.0000,-10.0000)
preview_dimension = (480,278)

def get_preview_dimension(bbox):
    if isinstance(bbox,str):
        bbox = json.loads(bbox)
    bbox_ratio = [(bbox[2] - bbox[0]) / (australia_bbox[2] - australia_bbox[0]),(bbox[3] - bbox[1]) / (australia_bbox[3] - australia_bbox[1])]
    if (bbox_ratio[0] > bbox_ratio[1]):
        return (preview_dimension[0],int(math.floor(preview_dimension[1] * (bbox_ratio[1] / bbox_ratio[0]))))
    else:
        return (int(math.floor(preview_dimension[0] * bbox_ratio[0] * (1 / bbox_ratio[1]))),preview_dimension[1])
    


task_layer_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

base_preview_url = lambda geoserver_url:geoserver_url + "/ows?service=wms&version=1.1.0&request=GetMap&srs=EPSG%3A4326&format=image/png"

def layer_preview_urls(sync_job,geoserver_url):
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
                link = geoserver_url + link[link.find("/geoserver") + len("/geoserver"):]
                if resource["format"] == "image/jpeg":
                    urls.append((link,".jpg",height))
                else:
                    urls.append((link,".{}".format(resource["format"][6:]),height))
        urls.sort(key=lambda data:data[2])
    #add GetMap url 
    bbox = sync_job.get("bbox") or australia_bbox
    dimension = get_preview_dimension(bbox)
    preview_url = base_preview_url(geoserver_url)  + "&bbox=" + ",".join([str(d) for d in bbox]) + "&layers=" + "%3A".join([sync_job["workspace"],sync_job['name']]) + "&width=" + str(dimension[0]) + "&height=" + str(dimension[1])
    
    urls.append( (preview_url,".png",-1))

    return urls

def _get_layer_preview(sync_job,task_metadata,task_status):
    logger.info("Try to get layer preview image")
    urls = layer_preview_urls(sync_job,settings.GEOSERVER_URL[0])
    index = 0
    for url in urls:
        index += 1
        resp = requests.get(url[0], auth=(settings.GEOSERVER_USERNAME[0],settings.GEOSERVER_PASSWORD[0]),timeout=60)
        if resp.status_code >= 400:
            task_status.del_message("preview_file")
            if index == len(urls):
                raise Exception("Failed to get layer's preview image. ({0}: {1})".format(resp.status_code, resp.content))
            else:
                continue
        elif not resp.headers['Content-Type'].startswith("image/"):
            task_status.del_message("preview_file")
            if resp.text.find("This request used more time than allowed") >= 0:
                #processing timeout; next time, has high change to succeed becuare the map is prepared by previous request
                if index == len(urls):
                    raise Exception("Get layer's preview image timeout.")
                else:
                    continue
            else:
                if index == len(urls):
                    raise Exception("url:{}\r\n{}".format(url[0],resp.text))
                else:
                    continue
        #getmap succeed
        try:
            folder = os.path.join(settings.PREVIEW_ROOT_PATH,settings.SLAVE_NAME,sync_job["channel"],sync_job["workspace"])
            if not os.path.exists(folder):   os.makedirs(folder)
            filename = os.path.join(folder,sync_job["name"] + url[1])

            with open(filename, 'wb') as fd:
                for chunk in resp.iter_content(1024):
                    fd.write(chunk)

            task_status.set_message("preview_file",filename[len(settings.PREVIEW_ROOT_PATH) + 1:])
            break
        except:
            raise Exception("Failed to get layer's preview image. {0}".format(resp.content))

def _get_layer_preview_with_dependent_geoservers(sync_job,task_metadata,task_status):
    logger.info("Try to get layer preview image")

    exceptions = []
    for i in range(len(settings.GEOSERVER_URL)):
        stagename = settings.GEOSERVER_HOST[i]
        try:
            if task_status.is_stage_not_succeed(stagename):
                urls = layer_preview_urls(sync_job,geoserver_url=settings.GEOSERVER_URL[i])
                index = 0
                for url in urls:
                    index += 1
                    resp = requests.get(url[0], auth=(settings.GEOSERVER_USERNAME[i],settings.GEOSERVER_PASSWORD[i]),timeout=60)
                    if resp.status_code >= 400:
                        if index == len(urls):
                            raise Exception("Failed to get layer's preview image from '{2}'. ({0}: {1})".format(resp.status_code, resp.content,settings.GEOSERVER_HOST[i]))
                        else:
                            continue
                    elif not resp.headers['Content-Type'].startswith("image/"):
                        if resp.text.find("This request used more time than allowed") >= 0:
                            #processing timeout; next time, has high change to succeed becuare the map is prepared by previous request
                            if index == len(urls):
                                raise Exception("Get layer's preview image from '{}' timeout.".format(settings.GEOSERVER_HOST[i]))
                            else:
                                continue
                        else:
                            if index == len(urls):
                                raise Exception("{2}: url:{0}\r\n{1}".format(url[0],resp.text,settings.GEOSERVER_HOST[i]))
                            else:
                                continue
                    #getmap succeed
                    try:
                        folder = os.path.join(settings.PREVIEW_ROOT_PATH,settings.SLAVE_NAME,sync_job["channel"],sync_job["workspace"])
                        if not os.path.exists(folder):   os.makedirs(folder)
                        if i == 0:
                            filename = os.path.join(folder,sync_job["name"] + url[1])
                        else:
                            filename = os.path.join(folder,"{0}-{2}{1}".format(sync_job["name"] ,url[1],settings.GEOSERVER_HOST[i]))
            
                        with open(filename, 'wb') as fd:
                            for chunk in resp.iter_content(1024):
                                fd.write(chunk)
            
                        task_status.set_stage_message(stagename,"preview_file",filename[len(settings.PREVIEW_ROOT_PATH) + 1:])
                        task_status.del_stage_message(stagename,"message")
                        task_status.stage_succeed(stagename)
                        if i == 0:
                            task_status.set_message("preview_file",filename[len(settings.PREVIEW_ROOT_PATH) + 1:])
                        break
                    except:
                        raise Exception("Failed to get layer's preview image from '{1}'. {0}".format(traceback.format_exc(),settings.GEOSERVER_HOST[i]))
        except:
            task_status.stage_failed(stagename)
            task_status.del_stage_message(stagename,"preview_file")
            task_status.set_stage_message(stagename,"message",str(sys.exc_info()[1]))
            if i == 0:
                task_status.del_message("preview_file")
            exceptions.append(str(sys.exc_info()[1]))


    if exceptions:
        raise Exception("\n".join(exceptions))
    elif task_status.all_stages_succeed:
        task_status.clean_task_failed()
    else:
        task_status.task_failed()

if len(settings.GEOSERVER_URL) > 1:
    get_layer_preview = _get_layer_preview_with_dependent_geoservers
else:
    get_layer_preview = _get_layer_preview

tasks_metadata = [
                    ("get_layer_preview", update_feature_job, layer_preview_task_filter      , task_layer_name  , _get_layer_preview),
                    ("get_layer_preview", update_livelayer_job, layer_preview_task_filter      , task_layer_name  , _get_layer_preview),
                    ("get_layer_preview", update_wmslayer_job, layer_preview_task_filter      , task_layer_name  , _get_layer_preview),
]

