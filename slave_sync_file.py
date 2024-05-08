import logging
import os
import subprocess
import json
import traceback

from slave_sync_env import (
    BORGCOLLECTOR_SSH,env,SLAVE_NAME,PUBLISH_PATH,CACHE_PATH,
    PREVIEW_ROOT_PATH,SYNC_PATH,SYNC_SERVER,
    SHARE_LAYER_DATA,SHARE_PREVIEW_DATA,
    parse_remotefilepath,BORGCOLLECTOR_SERVER,BORGCOLLECTOR_DOWNLOAD_PATH,BORGCOLLECTOR_PREVIEW_PATH,
    now
)
from slave_sync_task import (
    db_feature_task_filter,gs_style_task_filter,gs_spatial_task_filter,layer_preview_task_filter,

    update_wmsstore_job,update_wmslayer_job,remove_wmslayer_job,remove_wmsstore_job,
    update_livestore_job,update_livelayer_job,remove_livelayer_job,remove_livestore_job,
    empty_gwc_layer_job,empty_gwc_group_job,empty_gwc_livelayer_job,
    update_feature_job,remove_feature_job,update_feature_metadata_job,empty_gwc_feature_job,update_workspace_job
)

logger = logging.getLogger(__name__)


task_name = lambda sync_job: "{0}:{1}".format(sync_job["workspace"],sync_job["name"]) if "workspace" in sync_job else (sync_job["name"] if "name" in sync_job else sync_job["schema"])

download_cmd = ["rsync", "-Paz", "-e", BORGCOLLECTOR_SSH,None,None]
md5_cmd = BORGCOLLECTOR_SSH.split() + [None,"md5sum",None]
local_md5_cmd = ["md5sum",None]

def check_file_md5(md5_cmd,md5,task_status = None):
    logger.info("Executing {}...".format(repr(md5_cmd)))
    get_md5 = subprocess.Popen(md5_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    get_md5_output = get_md5.communicate()

    if get_md5.returncode != 0:
        raise Exception("{0}:{1}".format(get_md5.returncode,get_md5_output[1]))
    elif get_md5_output[1] and get_md5_output[1].strip():
        logger.info("stderr: {}".format(get_md5_output[1]))
        if task_status:
            task_status.set_message("message",get_md5_output[1])


    file_md5 = get_md5_output[0].split()[0].decode()
    if file_md5 != md5:
        raise Exception("md5sum checks failed.Expected md5 is {0}; but file's md5 is {1}".format(md5,file_md5))


def download_file(remote_path,local_path,task_status = None,md5=None):
    if BORGCOLLECTOR_SERVER and BORGCOLLECTOR_DOWNLOAD_PATH:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}{}".format(BORGCOLLECTOR_SERVER,BORGCOLLECTOR_DOWNLOAD_PATH,remote_path.split(":",1)[1].split("download",1)[1])
        else:
            remote_path = "{}{}".format(BORGCOLLECTOR_DOWNLOAD_PATH,remote_path.split("download",1)[1])
    elif BORGCOLLECTOR_SERVER:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}".format(BORGCOLLECTOR_SERVER,remote_path.split(":",1)[1])
    elif BORGCOLLECTOR_DOWNLOAD_PATH:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}{}".format(remote_path.split(":",1)[0],BORGCOLLECTOR_DOWNLOAD_PATH,remote_path.split(":",1)[1].split("download",1)[1])
        else:
            remote_path = "{}{}".format(BORGCOLLECTOR_DOWNLOAD_PATH,remote_path.split("download",1)[1])

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

    if rsync.returncode != 0:
        raise Exception("{0}:{1}".format(rsync.returncode,rsync_output[1]))
    elif rsync_output[1] and rsync_output[1].strip():
        logger.info("stderr: {}".format(rsync_output[1]))
        if task_status:
            task_status.set_message("message",rsync_output[1])


    if md5:
        #check file md5 after downloading
        local_md5_cmd[len(local_md5_cmd) - 1] = local_path
        check_file_md5(local_md5_cmd,md5,task_status)

def load_metafile(sync_job):
    meta_file = sync_job.get('meta',None)
    if not meta_file:
        #no meta file, all meta datas are embeded into the sync_job
        return

    task_status = sync_job['status'].get_task_status("load_metadata")

    try:
        if task_status.is_succeed: 
            #this task has been executed successfully,
            #load the json file and add the meta data into sync_job
            local_meta_file = task_status.get_message("meta_file")
            with open(local_meta_file,"r") as f:
                meta_data = json.loads(f.read())
            sync_job.update(meta_data)
            sync_job['meta']['local_file'] = local_meta_file
            return
    except:
        pass

    logger.info("Begin to load meta data for job({})".format(sync_job['job_file']))
    task_status.last_process_time = now()
    if SHARE_LAYER_DATA:
        sync_job['meta']['local_file'] = parse_remotefilepath(sync_job["meta"]["file"])["file"]
        meta_data = None
        with open(sync_job['meta']['local_file'],"r") as f:
            meta_data = json.loads(f.read())
        sync_job.update(meta_data)
        task_status.set_message("message","Find the meta file from shared layer data.")
        task_status.set_message("meta_file",sync_job['meta']['local_file'])
        task_status.succeed()
        return

    #download from borg master
    temp_file = os.path.join(CACHE_PATH,"job.meta.json")
    if sync_job['action'] == "remove":
        #for remove action, doesn't check md5 because of the following case. 
        # 1. Publish an layer to repository
        # 2. Slave pull from the repository and then publish the layer, at this time, the md5 of the layer's metadata file is 'A'
        # 3. Publish the layer again, and now, the md5 of the layer's metadata file is 'B',
        # 4. Remove the layer from repository.
        # 5. Slave pull from the repository, only the last version will be fetched, and intermediate versions are ignored. so the publish action in step 3 is invisible for slave cient.
        # 6. Slave client try to fetch the meta file from master and compare the md5 , and found: file's md5 is 'B', but md5 data in repository is 'A', doesn't match.
        download_file(meta_file["file"],temp_file,task_status,None)
    else:
        download_file(meta_file["file"],temp_file,task_status,meta_file.get('md5',None))
    meta_data = None
    with open(temp_file,"r") as f:
        meta_data = json.loads(f.read())
    sync_job.update(meta_data)
    if "workspace" in sync_job:
        local_meta_file = os.path.join(CACHE_PATH,"{}.{}.meta.json".format(sync_job["workspace"],sync_job["name"]))
    else:
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

def load_table_dumpfile(sync_job):
    if sync_job["action"] != "publish":
        #not a publish job, no need to download table data
        return

    data_file = sync_job.get('data',None)
    if not data_file:
        raise Exception("Can't find data file in json file.")
    if SYNC_SERVER:
        #download from local slave
        download_file("{0}:{1}/{2}.tar".format(SYNC_SERVER,SYNC_PATH,sync_job["name"]),data_file['local_file'],None,data_file.get("md5",None))
    else:
        #download from borg master
        download_file(data_file["file"],data_file['local_file'],None,data_file.get('md5',None))

def load_gs_stylefile(sync_job,task_metadata,task_status):
    if sync_job["action"] == "remove":
        #remove task, no need to download style file
        return
    style_files = sync_job.get('styles',None)
    if not style_files: 
        return
    for name,style_file in style_files.items():
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


upload_cmd = ["rsync", "-azR" ,"-e", BORGCOLLECTOR_SSH,None,None]

def upload_file(local_file,remote_path,task_status):
    if BORGCOLLECTOR_SERVER and BORGCOLLECTOR_PREVIEW_PATH:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}{}".format(BORGCOLLECTOR_SERVER,BORGCOLLECTOR_PREVIEW_PATH,remote_path.split(":",1)[1].split("preview",1)[1])
        else:
            remote_path = "{}{}".format(BORGCOLLECTOR_PREVIEW_PATH,remote_path.split("preview",1)[1])
    elif BORGCOLLECTOR_SERVER:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}".format(BORGCOLLECTOR_SERVER,remote_path.split(":",1)[1])
    elif BORGCOLLECTOR_PREVIEW_PATH:
        if remote_path.find("@") > 0:
            remote_path = "{}:{}{}".format(remote_path.split(":",1)[0],BORGCOLLECTOR_PREVIEW_PATH,remote_path.split(":",1)[1].split("preview",1)[1])
        else:
            remote_path = "{}{}".format(BORGCOLLECTOR_PREVIEW_PATH,remote_path.split("preview",1)[1])

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
    if sync_job["status"].get_task_status("get_layer_preview").get_message("preview_file"):
        local_file = os.path.join(PREVIEW_ROOT_PATH,".",sync_job["status"].get_task_status("get_layer_preview").get_message("preview_file"))
        upload_file(local_file,sync_job["preview_path"],task_status)
        task_status.set_message("message","Upload file {} to {}".format(local_file,sync_job["preview_path"]))
    else:
        task_status.task_failed()
        task_status.set_message("message","Uploading preview image file is ignored because preview image has not been generated for some reason.")

def delete_table_dumpfile(sync_job):
    f = sync_job.get('data',{"local_file":None})['local_file']
    if f and os.path.exists(f):
        os.remove(f)

def delete_dumpfile(sync_job,task_metadata,task_status):
    """
    Delete the dump files
    1. table data file
    2. meta file
    3. style files
    """
    messages = [] 
    for f in [local_file for local_file in (
        [sync_job.get('data',{}).get('local_file')] + 
        [sync_job.get('meta',{}).get('local_file')] + 
        [style_file.get('local_file') for style_file in (sync_job.get('styles') or {}).values() ]
        ) if local_file ]:
        try:
            if os.path.exists(f):
                os.remove(f)
                messages.append("Succeed to remove file({}).".format(f))
        except:
            message = traceback.format_exc()
            logger.error("Remove file ({}) failed. {}".format(f,message))
            messages.append("Failed to remove file({}). {}".format(f,message))
            task_status.task_failed()

        task_status.set_message("message",os.linesep.join(messages))

if not SHARE_LAYER_DATA and not SHARE_PREVIEW_DATA:
    tasks_metadata = [
                        #("load_table_dumpfile", update_feature_job, db_feature_task_filter      , task_name, load_table_dumpfile),
                        ("load_gs_stylefile"  , update_feature_job, gs_style_task_filter, task_name, load_gs_stylefile),
                        ("load_gs_stylefile"  , update_livelayer_job, gs_style_task_filter, task_name, load_gs_stylefile),
                        ("load_gs_stylefile"  , update_feature_metadata_job, gs_style_task_filter, task_name, load_gs_stylefile),
                        ("send_layer_preview"  , update_feature_job, layer_preview_task_filter, task_name, send_layer_preview),
                        ("send_layer_preview"  , update_livelayer_job, layer_preview_task_filter, task_name, send_layer_preview),
                        ("send_layer_preview"  , update_wmslayer_job, layer_preview_task_filter, task_name, send_layer_preview),
    
                        ("delete_dumpfile"  , update_wmsstore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_wmslayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_wmsstore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_wmslayer_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_livestore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_livelayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_livestore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_livelayer_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , empty_gwc_layer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_group_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_livelayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_feature_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_feature_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_feature_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_feature_metadata_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_workspace_job, None, task_name, delete_dumpfile),
    ]
elif SHARE_LAYER_DATA and SHARE_PREVIEW_DATA:
    tasks_metadata = []
elif SHARE_LAYER_DATA:
    tasks_metadata = [
                        ("send_layer_preview"  , update_feature_job, layer_preview_task_filter, task_name, send_layer_preview),
                        ("send_layer_preview"  , update_livelayer_job, layer_preview_task_filter, task_name, send_layer_preview),
                        ("send_layer_preview"  , update_wmslayer_job, layer_preview_task_filter, task_name, send_layer_preview)
    ]
elif SHARE_PREVIEW_DATA:
    tasks_metadata = [
                        #("load_table_dumpfile", update_feature_job, db_feature_task_filter      , task_name, load_table_dumpfile),
                        ("load_gs_stylefile"  , update_feature_job, gs_style_task_filter, task_name, load_gs_stylefile),
                        ("load_gs_stylefile"  , update_livelayer_job, gs_style_task_filter, task_name, load_gs_stylefile),
                        ("load_gs_stylefile"  , update_feature_metadata_job, gs_style_task_filter, task_name, load_gs_stylefile),
    
                        ("delete_dumpfile"  , update_wmsstore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_wmslayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_wmsstore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_wmslayer_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_livestore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_livelayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_livestore_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_livelayer_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , empty_gwc_layer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_group_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_livelayer_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , empty_gwc_feature_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_feature_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , remove_feature_job, None, task_name, delete_dumpfile),
                        ("delete_dumpfile"  , update_feature_metadata_job, None, task_name, delete_dumpfile),
    
                        ("delete_dumpfile"  , update_workspace_job, None, task_name, delete_dumpfile),
    ]
