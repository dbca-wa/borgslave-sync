import os
import traceback
import subprocess
import logging
import pytz
import socket
from datetime import datetime,date,timedelta
import time

from slave_sync_task import (
    update_feature_job,update_feature_metadata_job,remove_feature_job,update_auth_job,update_access_rules_job,
    update_wmsstore_job,update_wmslayer_job,remove_wmslayer_job,remove_wmsstore_job,
    update_layergroup_job,remove_layergroup_job,
    JOB_DEF_INDEX,JOB_TYPE_INDEX,jobname,
    empty_gwc_layer_job,empty_gwc_group_job,
)
from slave_sync_env import (
    CODE_BRANCH,LISTEN_CHANNELS,get_version,SLAVE_NAME
)

PATH = os.path.dirname(os.path.realpath(__file__))
CACHE_PATH = os.path.join(PATH,'.sync_status')

MASTER_PGSQL_HOST = os.environ.get("MASTER_PGSQL_HOST")
MASTER_PGSQL_DATABASE = os.environ.get("MASTER_PGSQL_DATABASE")
MASTER_PGSQL_SCHEMA = os.environ.get("MASTER_PGSQL_SCHEMA","public")
MASTER_PGSQL_PORT = os.environ.get("MASTER_PGSQL_PORT","5432")
MASTER_PGSQL_USERNAME = os.environ.get("MASTER_PGSQL_USERNAME")
MASTER_PGSQL_PASSWORD = os.environ.get("MASTER_PGSQL_PASSWORD")

feedback_disabled = not all([MASTER_PGSQL_HOST,MASTER_PGSQL_DATABASE,MASTER_PGSQL_SCHEMA,MASTER_PGSQL_PORT,MASTER_PGSQL_USERNAME,SLAVE_NAME])
logger = logging.getLogger(__name__)

if not feedback_disabled:
    sql_cmd = ["psql","-h",MASTER_PGSQL_HOST,"-p",MASTER_PGSQL_PORT,"-d",MASTER_PGSQL_DATABASE,"-U",MASTER_PGSQL_USERNAME,"-w","-c",None]
    env = os.environ.copy()
    env["PGPASSWORD"] = MASTER_PGSQL_PASSWORD
   
class SlaveServerSyncNotify(object):
    _failed_sql_file_template = os.path.join(CACHE_PATH,'failed_response_{0}.sql')

    @classmethod
    def _current_failed_sql_file(cls):
        """
        Return the current sql file for saving the failed sql
        """
        return cls._failed_sql_file_template.format(date.today().strftime('%Y-%m-%d'))
        
    @classmethod
    def _failed_sql_files(cls):
        """
        Return the active failed sql files which need to be executed again.
        Only the failed sql occured in recent two days' are required to execute; this can prevent non successful sql from being executed for ever.
        """
        t = date.today()
        return [cls._failed_sql_file_template.format(d.strftime('%Y-%m-%d')) for d in [t - timedelta(1),t]]

    _sql_separator='/*-------------------------------------------------------------------------------------------------------------------------------*/'
    @classmethod
    def exec_failed_sql(cls):
        for sql_file in cls._failed_sql_files():
            if not os.path.exists(sql_file):
                #no failed feedbacks
                continue

            failed_sqls = []
            with open(sql_file,'r') as f:
                for sql in f.read().split(cls._sql_separator):
                    if not sql.strip(): continue
                    sql_cmd[len(sql_cmd) - 1] = sql
                    sql_process = subprocess.Popen(sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
                    sql_output = sql_process.communicate()
                    if sql_output[1] and sql_output[1].strip():
                        logger.info("stderr: {}".format(sql_output[1]))

                    if sql_process.returncode != 0:
                        logger.error("Execute failed sql failed with return code ({0})".format(sql_process.returncode))
                        failed_sqls.append(sql.strip())
            if failed_sqls:
                #some feedbacks are executed failed
                with open(sql_file,'w') as f:
                    f.write("{0}{1}{0}".format(os.linesep,cls._sql_separator).join(failed_sqls))
            else:
                #failed feedbacks are executed successfully, remove the failed feedbacks.
                os.remove(sql_file)

    @classmethod
    def _save_failed_sql(cls,sql):
        f_name = cls._current_failed_sql_file()
        f_exists = os.path.exists(f_name)
        with open(f_name,'a') as f:
            if f_exists:
                f.write("{1}{2}{1}{0}".format(sql,os.linesep,cls._sql_separator))
            else:
                f.write(sql)
    
    @classmethod
    def send_last_poll_time(cls):
        if feedback_disabled: return
        if not hasattr(cls,"_listen_channels"):
            cls._listen_channels = ",".join(LISTEN_CHANNELS)
        last_poll_time = datetime.utcfromtimestamp(time.mktime(datetime.now().timetuple())).replace(tzinfo=pytz.UTC)
        sql = """
DO 
$$BEGIN
    IF EXISTS (SELECT 1 FROM {0}.monitor_slaveserver WHERE name='{1}') THEN
        UPDATE {0}.monitor_slaveserver SET listen_channels='{2}', last_poll_time='{3}',code_version='{4}' WHERE name='{1}';
    ELSE
        INSERT INTO {0}.monitor_slaveserver (name,register_time,listen_channels,last_poll_time,code_version) VALUES ('{1}','{3}','{2}','{3}','{4}');
    END IF;
END$$;
""".format(MASTER_PGSQL_SCHEMA,SLAVE_NAME,cls._listen_channels,last_poll_time,"{0} ({1})".format(get_version(),CODE_BRANCH))
        sql_cmd[len(sql_cmd) - 1] = sql
        sql_process = subprocess.Popen(sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        sql_output = sql_process.communicate()
        if sql_output[1] and sql_output[1].strip():
            logger.info("stderr: {}".format(sql_output[1]))

        if sql_process.returncode != 0:
            logger.error("Update the last sync info in master db failed with return code ({0})".format(sql_process.returncode))


    @classmethod
    def send_last_sync_time(cls,pull_status):
        if feedback_disabled: return
        last_sync_time = datetime.utcfromtimestamp(time.mktime(datetime.now().timetuple())).replace(tzinfo=pytz.UTC)
        last_sync_message = str(pull_status).replace("'","''")
        sql = """
DO 
$$BEGIN
    UPDATE {0}.monitor_slaveserver SET last_sync_time='{2}', last_sync_message='{3}' WHERE name='{1}';
END$$;
""".format(MASTER_PGSQL_SCHEMA,SLAVE_NAME,last_sync_time,last_sync_message)

        #logger.info("hg pull status notify: \r\n" + sql)
        sql_cmd[len(sql_cmd) - 1] = sql
        sql_process = subprocess.Popen(sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        sql_output = sql_process.communicate()
        if sql_output[1] and sql_output[1].strip():
            logger.info("stderr: {}".format(sql_output[1]))

        if sql_process.returncode != 0:
            logger.error("Update the last sync info in master db failed with return code ({0})".format(sql_process.returncode))

    @classmethod
    def send_feature_sync_status(cls,task,remove=False):
        if feedback_disabled: 
            logger.info("Notify feature is disabled.")
            return
        try:
            sync_succeed = task["status"].is_succeed
            sync_message = str(task["status"]).replace("'","''")

            sync_time = task["status"].last_process_time or datetime.now()
            if sync_succeed:
                if remove:
                    #remove publish succeed.
                    sql_template = """
DELETE FROM {0}.monitor_publishsyncstatus AS b
USING {0}.monitor_slaveserver AS a
WHERE a.id = b.slave_server_id
  AND a.name = '{1}'
  AND b.publish = '{2}'
"""
                else:
                    #update publish succeed
                    sql_template = """
DO 
$$BEGIN
    IF EXISTS (SELECT 1 FROM {0}.monitor_slaveserver a JOIN monitor_publishsyncstatus b ON a.id=b.slave_server_id WHERE a.name='{1}' AND b.publish='{2}') THEN
        UPDATE {0}.monitor_publishsyncstatus AS b SET deploied_job_id='{3}', deploied_job_batch_id='{4}', deploy_message='{5}',deploy_time= {6}, sync_job_id = null, sync_job_batch_id = null, sync_time = null, sync_message = null,preview_file={7},spatial_type='{8}'
        FROM {0}.monitor_slaveserver AS a
        WHERE b.slave_server_id = a.id AND a.name='{1}' AND b.publish='{2}';
    ELSE
        INSERT INTO {0}.monitor_publishsyncstatus
            (slave_server_id,publish,deploied_job_id,deploied_job_batch_id,deploy_message,deploy_time,sync_job_id,sync_job_batch_id,sync_message,sync_time,preview_file,spatial_type) 
        SELECT id,'{2}','{3}','{4}','{5}',{6},null,null,null,null,{7},'{8}'
        FROM {0}.monitor_slaveserver
        WHERE name = '{1}';
    END IF;
END$$;
"""
            else:
                sql_template = """
DO 
$$BEGIN
    IF EXISTS (SELECT 1 FROM {0}.monitor_slaveserver a JOIN monitor_publishsyncstatus b ON a.id=b.slave_server_id WHERE a.name='{1}' AND b.publish='{2}') THEN
        UPDATE {0}.monitor_publishsyncstatus AS b SET sync_job_id='{3}', sync_job_batch_id='{4}', sync_message='{5}',sync_time= {6},spatial_type='{8}'
        FROM {0}.monitor_slaveserver AS a
        WHERE b.slave_server_id = a.id AND a.name='{1}' AND b.publish='{2}';
    ELSE
        INSERT INTO {0}.monitor_publishsyncstatus
            (slave_server_id,publish,deploied_job_id,deploied_job_batch_id,deploy_message,deploy_time,sync_job_id,sync_job_batch_id,sync_message,sync_time,spatial_type) 
        SELECT id,'{2}',null,null,null,null,'{3}','{4}','{5}',{6},'{8}'
        FROM {0}.monitor_slaveserver
        WHERE name = '{1}';
    END IF;
END$$;
"""

            sql = sql_template.format(MASTER_PGSQL_SCHEMA, SLAVE_NAME,task['name'], task["job_id"], task["job_batch_id"], sync_message, "'{0}'".format(sync_time) if sync_time else 'null',"'{0}'".format(task['preview_file']) if task.get('preview_file',None) else "null",task.get('spatial_type',''))

            #logger.info("Feature sync status notify: \r\n" + sql)
            sql_cmd[len(sql_cmd) - 1] = sql
            sql_process = subprocess.Popen(sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            sql_output = sql_process.communicate()
            if sql_output[1] and sql_output[1].strip():
                logger.info("stderr: {}".format(sql_output[1]))

            if sql_process.returncode != 0 and sql_output[1].find("ERROR") >= 0:
                cls._save_failed_sql(sql)
                logger.error("Update sync status of task ({0}) in master db failed with return code ({1})".format(task['job_file'],sql_process.returncode))       
        except:
            logger.error("Update sync status of task ({0}) in master db failed. {1}".format(task['job_file'],traceback.format_exc()))       


    @classmethod
    def send_job_sync_status(cls,task,task_metadata):
        if feedback_disabled: 
            logger.info("Notify feature is disabled.")
            return
        try:
            task_type = task_metadata[JOB_DEF_INDEX][JOB_TYPE_INDEX]
            task_name = jobname(task,task_metadata)
            action = task["action"]
            sync_succeed = task["status"].is_succeed
            sync_message = str(task["status"]).replace("'","''")

            sync_time = task["status"].last_process_time

            #update publish succeed
            sql_template = """
DO 
$$BEGIN
    IF EXISTS (SELECT 1 FROM {0}.monitor_slaveserver a JOIN monitor_tasksyncstatus b ON a.id=b.slave_server_id WHERE a.name='{1}' AND b.task_type='{2}' AND b.task_name='{3}') THEN
        UPDATE {0}.monitor_tasksyncstatus AS b SET action='{4}', sync_succeed={5}, sync_message='{6}',sync_time= {7}
        FROM {0}.monitor_slaveserver AS a
        WHERE b.slave_server_id = a.id AND a.name='{1}' AND b.task_type='{2}' AND b.task_name='{3}';
    ELSE
        INSERT INTO {0}.monitor_tasksyncstatus
            (slave_server_id,task_type,task_name,action,sync_succeed,sync_message,sync_time) 
        SELECT id,'{2}','{3}','{4}',{5},'{6}',{7}
        FROM {0}.monitor_slaveserver
        WHERE name = '{1}';
    END IF;
END$$;
"""
            sql = sql_template.format(MASTER_PGSQL_SCHEMA, SLAVE_NAME,task_type,task_name,action,sync_succeed, sync_message, "'{0}'".format(sync_time) if sync_time else 'null')

            #logger.info("Notify: \r\n" + sql)

            sql_cmd[len(sql_cmd) - 1] = sql
            sql_process = subprocess.Popen(sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            sql_output = sql_process.communicate()
            if sql_output[1] and sql_output[1].strip():
                logger.info("stderr: {}".format(sql_output[1]))

            if sql_process.returncode != 0 and sql_output[1].find("ERROR") >= 0:
                cls._save_failed_sql(sql)
                logger.error("Update sync status of task ({0}) in master db failed with return code ({1})".format(task['job_file'],sql_process.returncode))       
        except:
            logger.error("Update sync status of task ({0}) in master db failed. {1}".format(task['job_file'],traceback.format_exc()))       

task_name = lambda task: "{0}:{1}".format(task["workspace"],task["name"])


def send_update_feature_notify(sync_job,task_metadata):
    SlaveServerSyncNotify.send_feature_sync_status(sync_job,False)
    
def send_remove_feature_notify(sync_job,task_metadata):
    SlaveServerSyncNotify.send_feature_sync_status(sync_job,True)
    
def send_job_notify(sync_job,task_metadata):
    SlaveServerSyncNotify.send_job_sync_status(sync_job,task_metadata)
    
tasks_metadata = [
                    ("send_notify", update_feature_job, None, task_name, send_update_feature_notify),
                    ("send_notify", remove_feature_job, None, task_name, send_remove_feature_notify),


                    ("send_notify", update_feature_metadata_job   , None, task_name,send_job_notify),
                    ("send_notify", update_auth_job   , None, "update_roles",send_job_notify),
                    ("send_notify", update_access_rules_job, None, "update_access_rules", send_job_notify),
                    ("send_notify", update_wmsstore_job,None, task_name, send_job_notify),
                    ("send_notify", update_wmslayer_job,None, task_name, send_job_notify),
                    ("send_notify", remove_wmslayer_job, None, task_name, send_job_notify),
                    ("send_notify", remove_wmsstore_job, None, task_name, send_job_notify),
                    ("send_notify", remove_wmslayer_job, None, task_name, send_job_notify),
                    ("send_notify", empty_gwc_layer_job  , None, task_name, send_job_notify),
                    ("send_notify", empty_gwc_group_job  , None, task_name, send_job_notify),

                    ("send_notify", update_layergroup_job, None, task_name, send_job_notify),
                    ("send_notify", remove_layergroup_job, None, task_name, send_job_notify),
]

