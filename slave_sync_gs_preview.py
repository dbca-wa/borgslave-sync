import logging
import traceback
import math
import os
import requests

import geoserver_catalog_extension
from slave_sync_env import (
    PREVIEW_ROOT_PATH,SLAVE_NAME,
    GEOSERVER_URL,GEOSERVER_USERNAME,GEOSERVER_PASSWORD,
)

from slave_sync_task import (
    update_feature_job,gs_spatial_task_filter
)

logger = logging.getLogger(__name__)

australia_bbox = (108.0000,-45.0000,155.0000,-10.0000)
preview_dimension = (480,278)

def get_preview_dimension(bbox):
    bbox_ratio = [(bbox[2] - bbox[0]) / (australia_bbox[2] - australia_bbox[0]),(bbox[3] - bbox[1]) / (australia_bbox[3] - australia_bbox[1])]
    if (bbox_ratio[0] > bbox_ratio[1]):
        return (preview_dimension[0],int(math.floor(preview_dimension[1] * (bbox_ratio[1] / bbox_ratio[0]))))
    else:
        return (int(math.floor(preview_dimension[0] * bbox_ratio[0] * (1 / bbox_ratio[1]))),preview_dimension[1])
    


task_layer_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

base_preview_url = GEOSERVER_URL + "/ows?service=wms&version=1.1.0&request=GetMap&srs=EPSG%3A4326&format=image/png"

def get_layer_preview(sync_job,task_metadata,task_status):
    bbox = sync_job.get("bbox") or australia_bbox
    dimension = get_preview_dimension(bbox)
    url = base_preview_url  + "&bbox=" + ",".join([str(d) for d in bbox]) + "&layers=" + "%3A".join([sync_job["workspace"],sync_job['name']]) + "&width=" + str(dimension[0]) + "&height=" + str(dimension[1])

    logger.info("Try to get layer preview image, url = " + url)
    resp = requests.get(url, auth=(GEOSERVER_USERNAME,GEOSERVER_PASSWORD))
    if resp.status_code >= 400:
        raise Exception("Get layer's preview image failed. ({0}: {1})".format(resp.status_code, resp.content))
    elif resp.headers['Content-Type'] != "image/png":
        raise Exception(resp.text)
    try:
        folder = os.path.join(PREVIEW_ROOT_PATH,SLAVE_NAME,sync_job["channel"],sync_job["workspace"])
        if not os.path.exists(folder):   os.makedirs(folder)
        filename = os.path.join(folder,sync_job["name"] + ".png")

        with open(filename, 'wb') as fd:
            for chunk in resp.iter_content(1024):
                fd.write(chunk)
    except:
        raise Exception("Get layer's preview image failed. {0}".format(resp.content))


tasks_metadata = [
                    ("get_layer_preview", update_feature_job, gs_spatial_task_filter      , task_layer_name  , get_layer_preview),
]

