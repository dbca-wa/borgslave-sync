import os
import logging
import json

from slave_sync_env import (
    CACHE_PATH
)
from slave_sync_task import (
    update_feature_job,update_metadata_feature_job,remove_feature_job,update_livelayer_job,
)

logger = logging.getLogger(__name__)

task_name = lambda task: "{0}:{1}".format(task["workspace"],task["name"])

def prepare_feature(sync_job,task_metadata,task_status):
    #prepare the data file properties
    if "dump_path" in sync_job:
        #old version before 20160217,reform it to current version
        sync_job['data'] = {'file':sync_job["dump_path"],'md5':sync_job.get("data_md5",None)}

    if 'data' in sync_job:
        #have data file. populate the local cached file
        sync_job['data']['local_file'] = os.path.join(CACHE_PATH, "{}.{}.tar".format(sync_job["workspace"],sync_job["name"]))
    else:
        sync_job['data'] = {}

    #prepare the style file properties
    if 'style_path' in sync_job:
        #the version before 20160217, reform it to current version
        sync_job['styles'] = {'builtin':{'file':sync_job["style_path"],'md5':sync_job.get("style_md5",None)}}

    if 'styles' in sync_job and sync_job["styles"]:
        #have styles. populate the local cached file
        for name in sync_job['styles'].keys():
            if name == "builtin":
                sync_job['styles'][name]['local_file'] = os.path.join(CACHE_PATH, "{}.{}.sld".format(sync_job["workspace"],sync_job["name"]))
            else:
                sync_job['styles'][name]['local_file'] = os.path.join(CACHE_PATH, "{}.{}.{}.sld".format(sync_job["workspace"],sync_job["name"],name))
    else:
        sync_job['styles'] = {}


tasks_metadata = [
                    ("prepare", update_feature_job, None, task_name, prepare_feature),
                    ("prepare", remove_feature_job, None, task_name, prepare_feature),
                    ("prepare", update_metadata_feature_job   , None, task_name, prepare_feature),
                    ("prepare", update_livelayer_job   , None, task_name, prepare_feature),
]
