#!/usr/bin/env python
"""
Each job file will trigger a synchronizing job. and each job is splitted into a serial of ordered sync tasks
Each sync job has a sync status object which indicate whether the job is executed successfully or not. the status object is persisted into file system and can be survived across hg pull
Each sync task has also a sync task status object which indicate whether the task is executed successfully or not. the task  status object is contained by job status object

If different sync job contains a sync task with the same type and same name, then the task is called as a shared task, the task status object will be shared by sync jobs and this means this 
shared task only need to execute once, no matter whether it is executed successfully or not.

Each sync task has two argurments:
    1. sync_job: a dict contains the job data, which also contain the sync status object
    2. task_metadata: a tuple,contains the metadata of this task
    3. task_status: SlaveSyncTaskStatus object, the status object for this task

Each sync task is implemented in different modules:
    1. slave_sync_file.py : file transfer related tasks
    2. slave_sync_postgres.py : db related task
    3. slave_sync_gs.py: feature related tasks
    4. slave_sync_gs_wms.py: wms related tasks
    5. slave_sync_gs_layergroup.py: layergroup related tasks
    6. slave_sync_gs_gwc.py: gwc related tasks
    7. slave_sync_notify.py: send notify to master related tasks.

Each module has some requirements:
    1. a "logger object" used when need to log task realted information; if not present, the logger declared in this module is used.
    2. a "tasks_metadata" array object, which hook the sync task to sync job; if not paesent, no task defined in this module will be executed

Each sync job is defined with a tuple, all jobs are defined in slave_sync_task.py
    1.JOB_TYPE_INDEX : specify the job type
    2.JOB_NAME_INDEX : a constant or a function which has a "sync_job" argument
    3.CHANNEL_SUPPORT_INDEX : True if support channel; otherwise False
    4.JOB_FOLDER_INDEX : the folder which contains the job files in repository; if not in a specific folder, set to None
    5.JOB_ACTION_INDEX : "update" if file is added or updated in the repository; "remove" if file is removed from repository;if don't want to check action, set to None
    6.IS_JOB_INDEX : a function which has a file_name argument and check whether this file is a sync job. if don't want to check, set to None
    7.IS_VALID_JOB_INDEX : a function which has a sync_job argument and check whether this file contains a valid sync job. if don't want to check, set to None

Each sync  action is hooked into a sync job by included a tuple element in tasks_metadata array:
    1.TASK_TYPE_INDEX : specify the task type
    2.JOB_DEF_INDEX : specify a defined job tuple
    3.TASK_FILTER_INDEX: a function which has a sync_job argument; if return True,the task is included; if don't want filter function, set to None
    4.TASK_NAME_INDEX: a function which has a sync_job argument; return a task name; shared task is implemented by return the same name from different sync job
    5.TASK_HANDLER_INDEX: a function which has sync_job and task_status argurments; implement the task logic; execute succeed if return normally; failed if thrown a exception

All tasks will be contained by "sync_tasks" variable, which is declared in slave_sync_task.py

All tasks will be executed in predefined order defined by "ordered_sync_task_type", which is declared in slave_sync_task.py

Each sync task is a reusable program logic and can be used by different sync job.
Each sync job has a sync status object which contains task status object for each sync task 
"""
import json
import logging
import os
import traceback
import sys
import hglib

from slave_sync_env import (
    PATH,HG_NODE,LISTEN_CHANNELS,
    STATE_PATH,now,DEBUG,INCLUDE
)
from slave_sync_status import SlaveSyncStatus

from slave_sync_task import (
    sync_tasks,ordered_sync_task_type,
    TASK_TYPE_INDEX,JOB_DEF_INDEX,TASK_FILTER_INDEX,TASK_NAME_INDEX,TASK_HANDLER_INDEX,CHANNEL_SUPPORT_INDEX,JOB_FOLDER_INDEX,JOB_ACTION_INDEX,IS_JOB_INDEX,IS_VALID_JOB_INDEX,
    execute_task,taskname

)
import slave_sync_prepare
import slave_sync_gs_wms
import slave_sync_gs_layergroup
import slave_sync_gs
import slave_sync_gs_gwc
import slave_sync_gs_preview
import slave_sync_postgres
import slave_sync_file
import slave_sync_notify
import slave_catalogues

hg = hglib.open(STATE_PATH)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s %(levelname)s %(message)s',
)

sync_tasks_metadata = {}
notify_tasks_metadata = []
notify_tasks = []

prepare_tasks_metadata = []
prepare_tasks = []

module_init_handlers = []
module_reset_handlers = []

plugin_modules = [
        slave_sync_postgres,
        slave_sync_gs,
        slave_sync_gs_wms,
        slave_sync_gs_layergroup,
        slave_sync_gs_preview,
        slave_sync_gs_gwc,
        slave_sync_file,
        slave_catalogues
]
notify_modules = [
    slave_sync_notify
]
prepare_modules = [
    slave_sync_prepare
]

ignore_files = 0

for key in sync_tasks.keys():
    sync_tasks_metadata[key] = []

for m in plugin_modules:
    if hasattr(m,"tasks_metadata"): 
        for task_metadata in m.tasks_metadata:
            if task_metadata[TASK_TYPE_INDEX] not in sync_tasks_metadata:
                continue
            sync_tasks_metadata[task_metadata[TASK_TYPE_INDEX]].append((task_metadata,m.logger if hasattr(m,"logger") else logger))

    if hasattr(m,"initialize"):
        module_init_handlers.append(m.initialize)
    if hasattr(m,"reset"):
        module_reset_handlers.append(m.reset)

for m in notify_modules:
    if hasattr(m,"tasks_metadata"): 
        for task_metadata in m.tasks_metadata:
            if task_metadata[TASK_TYPE_INDEX] != "send_notify": continue
            notify_tasks_metadata.append(task_metadata)

for m in prepare_modules:
    if hasattr(m,"tasks_metadata"): 
        for task_metadata in m.tasks_metadata:
            if task_metadata[TASK_TYPE_INDEX] != "prepare": continue
            prepare_tasks_metadata.append(task_metadata)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s %(levelname)s %(message)s',
)

def previous(rev):
    return str(int(hg.log(rev)[0][0])-1)

def get_changeset():
    """
    Returns the accumulated set of changes between $HG_NODE and the current tip
    """
    changes = {}
    changes.update({k:v for (v,k) in hg.status(change=HG_NODE)})
    changes.update({k:v for (v,k) in hg.status(rev="{}:".format(HG_NODE))})
    return changes

def parse_job(file_name,action,file_content):
    """
    parse the task file content into a dict
    """
    if file_name.endswith(".json"):
        task = json.loads(file_content)
    else:
        task = {"job_file_content":file_content}

    #backward compatibility. set the action for meta.json file
    if "action" not in task and file_name.endswith(".meta.json"):
	task["action"] = "meta"

    task["job_file"] = file_name
    if action == "remove":
        if task.get("action","publish") != "publish":
            #a auxiliary task file is removed. no action is required.
            task["action"] = 'none'
        else:
            task["action"] = action
    elif "action" not in task:
        #set the action to default action 'publish'
        task["action"] = 'publish'
    return task

def sync():
    if DEBUG:
        logger.info("Run in debug mode.")
    if INCLUDE:
        logger.info("Only the files({}) will be processed.".format(",".join(INCLUDE)))
	
    try:
        for init_method in module_init_handlers:
            init_method()

        pull_status = SlaveSyncStatus.get_bitbucket_status()
        get_tasks(pull_status)
        slave_sync_notify.SlaveServerSyncNotify.send_last_sync_time(pull_status)

        logger.info("HG_NODE: {}".format(HG_NODE))
        for task_type in ordered_sync_task_type:
            for task_name,task in sync_tasks[task_type].iteritems():    
                if isinstance(task,list):
                    #shared task
                    logger.info("Shared Task : {0}  {1} = [{2}]".format(task_type,task_name,",".join([t[0]['job_file'] for t in task])))
                else:
                    #unshared task
                    logger.info("Task : {0}  {1} = {2}".format(task_type,task_name,task[0]['job_file']))
        for task in notify_tasks:
            logger.info("Task : {0}  {1} = {2}".format("send_notify",taskname(task[0],task[1]),task[0]['job_file']))

        #prepare tasks
        for task in prepare_tasks:
            logger.info("Begin to prepare task ({0}).".format(taskname(task[0],task[1])))
            try:
                task[1][TASK_HANDLER_INDEX](*task)
                logger.info("Succeed to prepare task ({0}).".format(taskname(task[0],task[1])))
            except:
                logger.error("Failed to prepare task ({0}). {1}".format(taskname(task[0],task[1]),traceback.format_exc()))

        #execute tasks
        for task_type in ordered_sync_task_type:
            for task in sync_tasks[task_type].values():    
                if isinstance(task,list):
                    #shared task
                    for shared_task in task:
                        execute_task(*shared_task)
                else:
                    #unshared task
                    execute_task(*task)

        if SlaveSyncStatus.all_succeed():
            logger.info("All done!")
        else:
            raise Exception("Some files({0}) are processed failed.".format(' , '.join([s.file for s in SlaveSyncStatus.get_failed_status_objects()])))

        if ignore_files:
            raise Exception("{} files are ignored in debug mode,rollback!".format(ignore_files))
        #raise Exception("Rollback for testing")
        return
    finally:
        #save notify status 
        SlaveSyncStatus.save_all()
        #send notify
        for task in notify_tasks:
            logger.info("Begin to send notify for task ({0}).".format(taskname(task[0],task[1])))
            try:
                task[1][TASK_HANDLER_INDEX](*task)
                logger.info("Succeed to send notify for task ({0}).".format(taskname(task[0],task[1])))
            except:
                logger.error("Failed to send notify for task ({0}). {1}".format(taskname(task[0],task[1]),traceback.format_exc()))
        #clear all tasks
        for k in sync_tasks.keys():
            sync_tasks[k].clear()

        for reset_method in module_reset_handlers:
            reset_method()

def is_sync_task(sync_job,segments,action,task_metadata):
    if task_metadata[JOB_DEF_INDEX][CHANNEL_SUPPORT_INDEX]:
        if not segments or len(segments) < 2:
            return False

        #channel support
        if segments[0] not in LISTEN_CHANNELS:
            #channel not lisened by this slave
            return False

        if task_metadata[JOB_DEF_INDEX][JOB_FOLDER_INDEX] and not segments[1] == task_metadata[JOB_DEF_INDEX][JOB_FOLDER_INDEX]:
            #check the job folder
            return False
    else:
        #not support channel
        if task_metadata[JOB_DEF_INDEX][JOB_FOLDER_INDEX] and not segments[0] == task_metadata[JOB_DEF_INDEX][JOB_FOLDER_INDEX]:
            #check the job folder
            return False

    if task_metadata[JOB_DEF_INDEX][JOB_ACTION_INDEX] and action != task_metadata[JOB_DEF_INDEX][JOB_ACTION_INDEX]:
        #The action is not equal with the action of this type
        return False

    if task_metadata[JOB_DEF_INDEX][IS_JOB_INDEX] and not task_metadata[JOB_DEF_INDEX][IS_JOB_INDEX](segments[len(segments) - 1]):
        #The job file is belonging to this job type
        return False

    if task_metadata[JOB_DEF_INDEX][IS_VALID_JOB_INDEX] and not task_metadata[JOB_DEF_INDEX][IS_VALID_JOB_INDEX](sync_job):
        #The job is a invalid job
        return False

    if task_metadata[TASK_FILTER_INDEX] and not task_metadata[TASK_FILTER_INDEX](sync_job):
        #The task is filtered out.
        return False

    return True
                    

def get_tasks(pull_status):
    global ignore_files
    changes = get_changeset()
    next_job = False
    for file_name, revision in changes.iteritems():
        if DEBUG and INCLUDE and file_name not in INCLUDE:
            #debug mode, file_name is not in INCLUDE
            ignore_files += 1
            continue
        tasks = {}
        try:
            segments = file_name.split('/',2)
            if revision in ['A','M']:
                action = "update"
                file_content = hg.cat([file_name],rev="tip")
            elif revision == 'R':
                action = "remove"
                pre_rev = previous(HG_NODE)
                try:
                    file_content = hg.cat([file_name],rev=pre_rev)
                except:
                    #can't get the file content
                    logger.error("Can't get file content. file = " + file_name)
                    pull_status.get_task_status(file_name).set_message("message","Failed to read file content, ignored.")
                    pull_status.get_task_status(file_name).set_message("action",action)
                    pull_status.get_task_status(file_name).succeed()
                    pull_status.get_task_status(file_name).last_process_time = now()
                    continue

            sync_job = parse_job(file_name,action,file_content)
            action = sync_job["action"]
            if action == 'none':
                #no action is required
                pull_status.get_task_status(file_name).set_message("message","No action is required.")
                pull_status.get_task_status(file_name).set_message("action","None")
                pull_status.get_task_status(file_name).succeed()
                pull_status.get_task_status(file_name).last_process_time = now()
                continue

            #tasks will be added only after if a sync job has some unexecuted task or unsuccessful task.
            job_failed = False
            next_job = False
            for task_type in ordered_sync_task_type:
                if task_type not in sync_tasks_metadata: continue
                if task_type not in sync_tasks:continue
                for (task_metadata,task_logger) in sync_tasks_metadata[task_type]:
                    try:
                        #if task_type == "update_access_rules":
                        #    import ipdb;ipdb.set_trace()
                        if not is_sync_task(sync_job,segments,action,task_metadata):
                            continue

                        if task_metadata[JOB_DEF_INDEX][CHANNEL_SUPPORT_INDEX]:
                            sync_job["channel"] = segments[0]
                    
                        if "status" not in sync_job:
                            #after a serial of checking, this is a sync task, set a status object to the task
                            sync_job["status"] = SlaveSyncStatus(file_name,action,file_content)

                        #This task meets all the conditions and is required to run
                        sync_job['status'].get_task_status(task_type).run = True

                        #check whether this task is already executed or not
                        if not job_failed and sync_job['status'].get_task_status(task_type).is_succeed:
                            #this task is already succeed, continue
                            break
            
                        #this task is not succeed or executed before, add this task to sync tasks
                        job_failed = True
                        task_name = taskname(sync_job,task_metadata)
                        if task_type not in tasks:
                            tasks[task_type] = {}
                        if task_name in sync_tasks[task_type]:
                            #task is already exist, this is a shared task
                            shared_task = sync_tasks[task_type][task_name]
                            if isinstance(shared_task,list):
                                task_status = shared_task[0][0]['status'].get_task_status(task_type)
                                tasks[task_type][task_name] = shared_task + [(sync_job,task_metadata,task_logger)]
                            else:
                                task_status = shared_task[0]['status'].get_task_status(task_type)
                                task_status.shared = True
                                tasks[task_type][task_name] = [shared_task,(sync_job,task_metadata,task_logger)]
                            tasks[task_type][task_name] = sorted(tasks[task_type][task_name], key=lambda x: x[0]['job_file'], reverse=True)
                            sync_job['status'].set_task_status(task_type,task_status)
                        else:
                            #init a default status object for this task
                            sync_job['status'].get_task_status(task_type)

                            tasks[task_type][task_name] = (sync_job,task_metadata,task_logger)

                        #if task_type == "create_workspace": raise Exception("Failed for testing.")
                        break
                    except:
                        #preprocess the file failed, continue to the next file
                        message = traceback.format_exc()
                        logger.error(message)
                        tasks.clear()
                        sync_job['status'].get_task_status(task_type).run = True
                        sync_job['status'].get_task_status(task_type).set_message("message","Preprocess the file failed. err = {0}".format(message))
                        sync_job['status'].get_task_status(task_type).failed()
                        #this job is failed, try to add a notify task
                        for notify_metadata in notify_tasks_metadata:
                            if is_sync_task(sync_job,segments,action,notify_metadata):
                                notify_tasks.append((sync_job,notify_metadata))
                                break
                        pull_status.get_task_status(file_name).set_message("action",action)
                        pull_status.get_task_status(file_name).set_message("message","Preprocess the file failed. err = {0}".format(message))
                        pull_status.get_task_status(file_name).failed()
                        pull_status.get_task_status(file_name).last_process_time = now()
                        next_job = True
                        break

                if next_job:
                    break

            if next_job:
                continue

            #add the sync job's tasks to the total sync tasks.
            for key,val in tasks.iteritems():
                sync_tasks[key].update(val)
            
            if tasks:
                #this job has some sync tasks to do, 
                #try to add a prepare task
                for task_metadata in prepare_tasks_metadata:
                    if is_sync_task(sync_job,segments,action,task_metadata):
                        prepare_tasks.append((sync_job,task_metadata))
                        break

                #try to add a notify task
                for task_metadata in notify_tasks_metadata:
                    if is_sync_task(sync_job,segments,action,task_metadata):
                        notify_tasks.append((sync_job,task_metadata))
                        break
                pull_status.get_task_status(file_name).set_message("message","Ready to synchronize")
            else :
                pull_status.get_task_status(file_name).set_message("message","Already synchronized or no need to synchronize")

            pull_status.get_task_status(file_name).set_message("action",action)
            pull_status.get_task_status(file_name).succeed()
            pull_status.get_task_status(file_name).last_process_time = now()
        except:
            pull_status.get_task_status(file_name).failed()
            message = traceback.format_exc()
            pull_status.get_task_status(file_name).set_message("message",message)
            pull_status.get_task_status(file_name).last_process_time = now()
            logger.error("Add the tasks for ({0}) failed.{1}".format(file_name,traceback.format_exc()))

if __name__ == "__main__":
    sync()
