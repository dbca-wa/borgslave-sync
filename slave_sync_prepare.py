import os
import logging
import json

from slave_sync_env import (
    CACHE_PATH
)
from slave_sync_task import (
    update_feature_job,update_metadata_feature_job,remove_feature_job,
)
from slave_sync_file import download_file

logger = logging.getLogger(__name__)

task_name = lambda task: "{0}:{1}".format(task["workspace"],task["name"])

def load_metafile(sync_job,task_metadata):
    meta_file = sync_job.get('meta',None)
    if not meta_file:
        #no meta file, all meta datas are embeded into the sync_job
        return

    task_name = taskname(sync_job,task_metadata)
    task_status = sync_job['status'].get_task_status("load_metdata")

    if task_status.is_succeed: 
        #this task has been executed successfully,
        #load the json file and add the meta data into sync_job
        local_meta_file = task_status.get_message("meta_file")
        with open(local_meta_file,"r") as f:
            meta_data = json.loads(f.read())
        sync_job.update(meta_data)
        sync_job['meta']['local_file'] = local_meta_file
        return

    logger.info("Begin to load meta data for job({})".format(sync_job['job_id']))
    #download from borg master
    temp_file = os.path.join(CACHE_PATH,"{}.meta.json".format(sync_job['job_id']))
    download_file(meta_file["file"],temp_file,task_status,meta_file.get('md5',None))
    meta_data = None
    with open(temp_file,"r") as f:
        meta_data = json.loads(f.read())
    sync_job.update(meta_data)
    local_meta_file = os.path.join(CACHE_PATH,"{}.meta.json".format(sync_job["name"]))
    try:
        os.remove(local_meta_file)
    except:
        #file not exist, ignore
        pass
    #rename to meta file
    os.rename(temp_file,local_meta_file)
    sync_job['meta']['local_file'] = local_meta_file
    task_status.set_message("message","Succeed to download meta data from master.")
    task_status.set_message("meta_file",local_meta_file)
    task_status.succeed()

def prepare_feature(sync_job,task_metadata):
    #load meta data first
    load_metafile(sync_job,task_metadata)

    #prepare the data file properties
    if "dump_path" in sync_job:
        #old version before 20160217,reform it to current version
        sync_job['data'] = {'file':sync_job["dump_path"],'md5':sync_job.get("data_md5",None)}

    if 'data' in sync_job:
        #have data file. populate the local cached file
        sync_job['data']['local_file'] = os.path.join(CACHE_PATH, "{}.tar".format(sync_job["name"]))
    else:
        sync_job['data'] = {}

    #prepare the style file properties
    if 'style_path' in sync_job:
        #the version before 20160217, reform it to current version
        sync_job['styles'] = {'builtin':{'file':sync_job["style_path"],'md5':sync_job.get("style_md5",None)}}

    if 'styles' in sync_job:
        #have styles. populate the local cached file
        for name in sync_job['styles'].keys():
            if name == "builtin":
                sync_job['styles'][name]['local_file'] = os.path.join(CACHE_PATH, "{}.sld".format(sync_job["name"]))
            else:
                sync_job['styles'][name]['local_file'] = os.path.join(CACHE_PATH, "{}.{}.sld".format(sync_job["name"],name))
    else:
        sync_job['styles'] = {}


tasks_metadata = [
                    ("prepare", update_feature_job, None, task_name, prepare_feature),
                    ("prepare", remove_feature_job, None, task_name, prepare_feature),
                    ("prepare", update_metadata_feature_job   , None, task_name, prepare_feature),
]
