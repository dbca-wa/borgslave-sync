import logging
import os
import subprocess
import json

from slave_sync_env import (
    BORG_SSH,env,SLAVE_NAME,PUBLISH_PATH,CACHE_PATH,
    PREVIEW_ROOT_PATH,SYNC_PATH,SYNC_SERVER
)
from slave_sync_task import (
    update_feature_job,update_metadata_feature_job,db_feature_task_filter,
    gs_style_task_filter,gs_spatial_task_filter
)

logger = logging.getLogger(__name__)


send_layer_preview_task_filter = lambda sync_job: gs_spatial_task_filter(sync_job) and sync_job.get("preview_path")

task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"])

download_cmd = ["rsync", "-Paz", "-e", BORG_SSH,None,None]
md5_cmd = BORG_SSH.split() + [None,"md5sum",None]
local_md5_cmd = ["md5sum",None]

def check_file_md5(md5_cmd,md5,task_status):
    logger.info("Executing {}...".format(repr(md5_cmd)))
    get_md5 = subprocess.Popen(md5_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    get_md5_output = get_md5.communicate()

    if get_md5_output[1] and get_md5_output[1].strip():
        logger.info("stderr: {}".format(get_md5_output[1]))
        task_status.set_message("message",get_md5_output[1])

    if get_md5.returncode != 0:
        raise Exception("{0}:{1}".format(get_md5.returncode,task_status.get_message("message")))

    file_md5 = get_md5_output[0].split()[0]
    if file_md5 != md5:
        raise Exception("md5sum checks failed.Expected md5 is {0}; but file's md5 is {1}".format(md5,file_md5))


def download_file(remote_path,local_path,task_status,md5=None):
    if md5:
        #check file md5 before downloading.
        remote_file_path = remote_path
        if remote_path.find("@") > 0:
            #remote_path includes user@server prefix,remote that prefix
            remote_file_path = remote_path.split(":",1)[1]
        if SYNC_SERVER:
            md5_cmd[len(md5_cmd) - 1] = remote_file_path
            md5_cmd[len(md5_cmd) - 3] = SYNC_SERVER
            check_file_md5(md5_cmd,md5,task_status)
        elif remote_path.find("@") > 0:
            md5_cmd[len(md5_cmd) - 1] = remote_file_path
            md5_cmd[len(md5_cmd) - 3] = remote_path.split(":",1)[0]
            check_file_md5(md5_cmd,md5,task_status)
        else:
            local_md5_cmd[len(local_md5_cmd) - 1] = remote_file_path
            check_file_md5(local_md5_cmd,md5,task_status)

    # sync over PostgreSQL dump with rsync
    download_cmd[len(download_cmd) - 2] = remote_path
    download_cmd[len(download_cmd) - 1] = local_path
    logger.info("Executing {}...".format(repr(download_cmd)))
    rsync = subprocess.Popen(download_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    rsync_output = rsync.communicate()
    if rsync_output[1] and rsync_output[1].strip():
        logger.info("stderr: {}".format(rsync_output[1]))
        task_status.set_message("message",rsync_output[1])

    if rsync.returncode != 0:
        raise Exception("{0}:{1}".format(rsync.returncode,task_status.get_message("message")))

    if md5:
        #check file md5 after downloading
        local_md5_cmd[len(local_md5_cmd) - 1] = local_path
        check_file_md5(local_md5_cmd,md5,task_status)

def load_metafile(sync_job):
    meta_file = sync_job.get('meta',None)
    if not meta_file:
        #no meta file, all meta datas are embeded into the sync_job
        return

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

    logger.info("Begin to load meta data for job({})".format(sync_job['job_file']))
    #download from borg master
    temp_file = os.path.join(CACHE_PATH,"job.meta.json")
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

def previous(rev):
    return str(int(hg.log(rev)[0][0])-1)

def load_table_dumpfile(sync_job,task_metadata,task_status):
    data_file = sync_job.get('data',None)
    if not data_file:
        raise Exception("Can't find data file in json file.")
    if SYNC_SERVER:
        #download from local slave
        download_file("{0}:{1}/{2}.tar".format(SYNC_SERVER,SYNC_PATH,sync_job["name"]),data_file['local_file'],task_status,data_file.get("md5",None))
	task_status.set_message("message","Succeed to download table data from slave server {0}".format(SYNC_SERVER))
    else:
        #download from borg master
        download_file(data_file["file"],data_file['local_file'],task_status,data_file.get('md5',None))
	task_status.set_message("message","Succeed to download table data from master.")

def load_gs_stylefile(sync_job,task_metadata,task_status):
    style_files = sync_job.get('styles',None)
    if not style_files: 
        return
    for name,style_file in style_files.iteritems():
        if SYNC_SERVER:
            #download from local slave
            
            if name == "builtin":
                download_file("{}:{}/{}.sld".format(SYNC_SERVER,SYNC_PATH,sync_job["name"]),style_file['local_file'],task_status,style_file.get("md5",None))
            else:
                download_file("{}:{}/{}.{}.sld".format(SYNC_SERVER,SYNC_PATH,sync_job["name"],name),style_file['local_file'],task_status,style_file.get("md5",None))
            task_status.set_message("message","Succeed to download style file from slave server {0}".format(SYNC_SERVER))
        else:
            #download from borg master
            download_file(style_file["file"],style_file['local_file'],task_status,style_file.get("md5",None))
            task_status.set_message("message","Succeed to download style file from master.")


upload_cmd = ["rsync", "-azR" ,"-e", BORG_SSH,None,None]

def upload_file(local_file,remote_path,task_status):
    # sync over PostgreSQL dump with rsync
    upload_cmd[len(upload_cmd) - 2] = local_file
    upload_cmd[len(upload_cmd) - 1] = remote_path
    logger.info("Executing {}...".format(repr(upload_cmd)))
    rsync = subprocess.Popen(upload_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    rsync_output = rsync.communicate()
    if rsync_output[1] and rsync_output[1].strip():
        logger.info("stderr: {}".format(rsync_output[1]))
        task_status.set_message("message",rsync_output[1])

    if rsync.returncode != 0:
        raise Exception("{0}:{1}".format(rsync.returncode,task_status.get_message("message")))

def send_layer_preview(sync_job,task_metadata,task_status):
    local_file = os.path.join(PREVIEW_ROOT_PATH,".",SLAVE_NAME,sync_job["channel"],sync_job["workspace"],sync_job["name"] + ".png")
    upload_file(local_file,sync_job["preview_path"],task_status)

    sync_job["preview_file"] = os.path.join(SLAVE_NAME,sync_job["channel"],sync_job["workspace"],sync_job["name"] + ".png")


tasks_metadata = [
                    ("load_table_dumpfile", update_feature_job, db_feature_task_filter      , task_name, load_table_dumpfile),
                    ("load_gs_stylefile"  , update_feature_job, gs_style_task_filter, task_name, load_gs_stylefile),
                    ("load_gs_stylefile"  , update_metadata_feature_job, gs_style_task_filter, task_name, load_gs_stylefile),
                    ("send_layer_preview"  , update_feature_job, send_layer_preview_task_filter, task_name, send_layer_preview),
]
