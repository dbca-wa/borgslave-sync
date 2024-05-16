import traceback
from slave_sync_env import (
    SKIP_AUTH,SKIP_GS,SKIP_DB,SKIP_RULES,
    FEATURE_FILTER,WMS_FILTER,LAYERGROUP_FILTER,now
)

TASK_TYPE_INDEX = 0
JOB_DEF_INDEX = 1
TASK_FILTER_INDEX = 2
TASK_NAME_INDEX = 3
TASK_HANDLER_INDEX = 4

JOB_TYPE_INDEX = 0
JOB_NAME_INDEX = 1
CHANNEL_SUPPORT_INDEX = 2
JOB_FOLDER_INDEX = 3
JOB_ACTION_INDEX = 4
IS_JOB_INDEX = 5
IS_VALID_JOB_INDEX = 6

json_task = lambda file_name: len(file_name.split("/")) == 1 and file_name.endswith(".json")
taskname = lambda task,task_metadata: task_metadata[TASK_NAME_INDEX](task) if hasattr(task_metadata[TASK_NAME_INDEX],"__call__") else task_metadata[TASK_NAME_INDEX]
jobname = lambda task,task_metadata: task_metadata[JOB_DEF_INDEX][JOB_NAME_INDEX](task) if hasattr(task_metadata[JOB_DEF_INDEX][JOB_NAME_INDEX],"__call__") else task_metadata[JOB_DEF_INDEX][JOB_NAME_INDEX]

sync_tasks = {
    "prepare": {},
    #"update_auth": {},
    "update_access_rules": {},
    
    "create_postgis_extension": {},
    "create_db_schema": {},
    "move_outdated_table": {},
    "restore_table": {},
    "restore_foreignkey": {},
    "create_access_view": {},
    "drop_outdated_table": {},
    "drop_table": {},

    "load_gs_stylefile": {},
    "create_workspace": {},
    "create_datastore": {},
    "delete_datastore": {},
    "delete_feature": {},
    "delete_style": {},
    "create_feature":{},
    "set_feature_styles":{},
    "create_style":{},

    "update_feature": {},
    "remove_feature": {},

    "update_wmsstore": {},
    "remove_wmsstore": {},

    "update_wmslayer": {},
    "remove_wmslayer": {},

    "update_layergroup": {},
    "remove_layergroup": {},

    "update_gwc":{},
    "empty_gwc":{},

    "reload_geoserver":{},

    "get_layer_preview":{},
    "send_layer_preview":{},

    "delete_dumpfile":{},
    "delete_dbfile":{},
    "purge_fastly_cache":{}

}
ordered_sync_task_type = [
            "create_postgis_extension",
            "create_db_schema",
            "move_outdated_table",
            "restore_table",
            "restore_foreignkey",
            "create_access_view",
            "drop_outdated_table",
            "drop_table",
            "delete_dbfile",

            #"update_auth",
            "update_access_rules",
            #"load_table_dumpfile",
            "load_gs_stylefile",
            "create_workspace",
            "delete_feature",
            "delete_datastore",
            "create_datastore",
            "update_wmsstore","update_wmslayer","update_layergroup","remove_layergroup","remove_wmslayer","remove_wmsstore",
            "create_feature","create_style","set_feature_styles",
            "update_gwc","empty_gwc",
            "reload_geoserver",
            "purge_fastly_cache",
            "get_layer_preview",
            "send_layer_preview",
            "delete_dumpfile"
]

#predefined sync_job filters
gs_task_filter = lambda sync_job: not SKIP_GS

gs_spatial_task_filter = lambda sync_job: not SKIP_GS and sync_job.get("sync_geoserver_data",True) and sync_job.get("spatial_data",False)
gs_feature_task_filter = lambda sync_job: not SKIP_GS and sync_job.get("sync_geoserver_data",True)
gs_style_task_filter = lambda sync_job : not SKIP_GS and sync_job.get("sync_geoserver_data",True) and any((key in sync_job) for key in ["styles","style_path"])

layer_preview_task_filter = lambda sync_job: gs_spatial_task_filter(sync_job) and sync_job.get("preview_path")

db_task_filter = lambda sync_job: not SKIP_DB
db_feature_task_filter = lambda sync_job: not SKIP_DB and sync_job.get("sync_postgres_data",True)
#no need to manage foreighkey for kmi channel
foreignkey_task_filter = lambda sync_job: db_feature_task_filter(sync_job) and sync_job["channel"] != "kmi"


#task definition for update access rules
valid_rules = lambda sync_job: not SKIP_RULES
update_access_rules_job = ("access_rules","geoserver access rules",True,None,"publish",lambda file_name: file_name == "layers.properties",valid_rules)

#task definition for update auth
valid_auth = lambda sync_job: not SKIP_AUTH
update_auth_job = ("auth","postgres and geoserver auth",False,None,"publish",lambda file_name: file_name == "slave_roles.sql",valid_auth)

#task definition for wms store and layers
required_wmsstore_attrs = [
    ("meta","action"),
    ("name", "workspace","capability_url","username","password","action") #to be compatible with tasks created before 20160330
]
valid_wmsstore = lambda l: WMS_FILTER(l) and any(all(key in l for key in store_attrs) for store_attrs in required_wmsstore_attrs)
update_wmsstore_job = ("wms_store",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"wms_stores","publish",json_task,valid_wmsstore)

required_wmsstore_remove_attrs = [
    ("name", "workspace","action"),
    ("meta","action") #to be compatible with tasks created before 20171127
]
valid_wmsstore_remove = lambda l: WMS_FILTER(l) and any(all(key in l for key in store_attrs) for store_attrs in required_wmsstore_remove_attrs)
remove_wmsstore_job = ("wms_store",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"wms_stores","remove",json_task,valid_wmsstore_remove)

required_wmslayer_attrs = [
    ("meta","action"),
    ("name", "workspace", "store","title","abstract","action") #to be compatible with tasks created before 20160330
]
valid_wmslayer = lambda l: WMS_FILTER(l) and any(all(key in l for key in layer_attrs) for layer_attrs in required_wmslayer_attrs)
update_wmslayer_job = ("wms_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"wms_layers","publish",json_task,valid_wmslayer)

required_wmslayer_remove_attrs = [
    ("name", "workspace", "store","action"),
    ("meta","action")  #to be compatible with tasks created before 20171127
]
valid_wmslayer_remove = lambda l: WMS_FILTER(l) and any(all(key in l for key in layer_attrs) for layer_attrs in required_wmslayer_remove_attrs)
remove_wmslayer_job = ("wms_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"wms_layers","remove",json_task,valid_wmslayer_remove)

#task definition for live datasource and live layers
required_livestore_attrs = [
    ("meta","action")
]
valid_livestore = lambda l: FEATURE_FILTER(l) and any(all(key in l for key in store_attrs) for store_attrs in required_livestore_attrs)
update_livestore_job = ("live_store",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"live_stores","publish",json_task,valid_livestore)

required_livestore_remove_attrs = [
    ("name", "workspace","action"),
    ("meta","action") #to be compatible with tasks created before 20171127
]
valid_livestore_remove = lambda l: FEATURE_FILTER(l) and any(all(key in l for key in store_attrs) for store_attrs in required_livestore_remove_attrs)
remove_livestore_job = ("live_store",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"live_stores","remove",json_task,valid_livestore_remove)

required_livelayer_attrs = [
    ("meta","action")
]
valid_livelayer = lambda l: FEATURE_FILTER(l) and any(all(key in l for key in layer_attrs) for layer_attrs in required_livelayer_attrs)
update_livelayer_job = ("live_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"live_layers","publish",json_task,valid_livelayer)

required_livelayer_remove_attrs = [
    ("name", "workspace","action"),
    ("meta","action")  #to be compatible with tasks created before 20171127
]
valid_livelayer_remove = lambda l: FEATURE_FILTER(l) and any(all(key in l for key in layer_attrs) for layer_attrs in required_livelayer_remove_attrs)
remove_livelayer_job = ("live_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"live_layers","remove",json_task,valid_livelayer_remove)

#task definition for layergroup
required_layergroup_attrs = ("name", "workspace","title","abstract","srs","layers","action")
valid_layergroup = lambda l: LAYERGROUP_FILTER(l) and all(key in l for key in required_layergroup_attrs)
update_layergroup_job = ("layergroup",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layergroups","publish",json_task,valid_layergroup)

required_layergroup_remove_attrs = ("name", "workspace","action")
valid_layergroup_remove = lambda l: LAYERGROUP_FILTER(l) and all(key in l for key in required_layergroup_remove_attrs)
remove_layergroup_job = ("layergroup",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layergroups","remove",json_task,valid_layergroup_remove)

required_empty_gwc_layer_attrs = ("name", "workspace","store","action")
valid_empty_gwc_layer = lambda l: WMS_FILTER(l) and all(key in l for key in required_empty_gwc_layer_attrs)
empty_gwc_layer_job = ("wms_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"wms_layers","empty_gwc",json_task,valid_empty_gwc_layer)
empty_gwc_livelayer_job = ("live_layer",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"live_layers","empty_gwc",json_task,valid_empty_gwc_layer)

required_empty_gwc_group_attrs = ("name", "workspace","action")
valid_empty_gwc_group = lambda l: LAYERGROUP_FILTER(l) and all(key in l for key in required_empty_gwc_group_attrs)
empty_gwc_group_job = ("layergroup",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layergroups","empty_gwc",json_task,valid_empty_gwc_group)

#task definition for features
required_feature_attrs = [
    ("job_id","meta","action"),
    ("job_id","name", "schema", "data_schema", "outdated_schema", "workspace", "data","action"), #to be compatible with tasks created before 20160318
    ("job_id","name", "schema", "data_schema", "outdated_schema", "workspace", "dump_path","action"), #to be compatible with tasks created before 20160217
]
valid_feature = lambda l: FEATURE_FILTER(l) and not l["job_file"].endswith(".meta.json") and any(all(key in l for key in feature_attrs) for feature_attrs in required_feature_attrs)
update_feature_job = ("feature",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layers","publish",json_task,valid_feature)

required_feature_remove_attrs = [
    ("name", "workspace","schema","data_schema","outdated_schema","action"),
    ("meta","action")  #to be compatible with tasks created before 20171127
]
valid_feature_remove = lambda l: FEATURE_FILTER(l) and not l["job_file"].endswith(".meta.json") and any(all(key in l for key in feature_attrs) for feature_attrs in required_feature_remove_attrs)
remove_feature_job = ("feature",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layers","remove",json_task,valid_feature_remove)

#task definition for feature's metadata
required_metadata_feature_attrs = [
    ("meta","action"),
    ("name","workspace","schema","action") #to be compatible with tasks created before 20160318
]
valid_metadata_feature_job = lambda l: FEATURE_FILTER(l) and l["job_file"].endswith(".meta.json") and any(all(key in l for key in feature_attrs) for feature_attrs in required_metadata_feature_attrs)
update_feature_metadata_job = ("feature",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layers","meta",json_task,valid_metadata_feature_job)

#task definition for empty feature's gwc
required_empty_gwc_feature_attrs = ("name","workspace","action")
valid_empty_gwc_feature_job = lambda l: FEATURE_FILTER(l) and all(key in l for key in required_empty_gwc_feature_attrs)
empty_gwc_feature_job = ("feature",lambda j:"{0}:{1}".format(j["workspace"],j["name"]),True,"layers","empty_gwc",json_task,valid_empty_gwc_feature_job)

#task definition for workspace
required_workspace_update_attrs = ("schema","auth_level","action","data_schema","outdated_schema")
valid_workspace_update_job = lambda l: FEATURE_FILTER(l) and all(key in l for key in required_workspace_update_attrs)
update_workspace_job = ("workspace",lambda j:j["schema"],True,"workspaces","publish",json_task,valid_workspace_update_job)

def get_http_response_exception(http_response):
    """
    return http response exception for logging
    """
    if hasattr(http_response,"reason"):
        if hasattr(http_response,"_content"):
            return "{0}({1})".format(http_response.reason,http_response._content)
        else:
            return http_response.reason
    elif hasattr(http_response,"content"):
        return http_response.content
    else:
        return ""


def get_task(task_type,task_name):
    """
    get task with specific type and name;
    return None if not found
    """
    try:
        return sync_tasks[task_type].get(task_name,None)
    except:
        return None

def execute_notify_task(sync_job,task_metadata,task_logger):
    """
    execute the notify task, based on tasks_metadata
    """
    task_name = taskname(sync_job,task_metadata)

    task_logger.info("Begin to process the notify task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"]))
    try:
        task_metadata[TASK_HANDLER_INDEX](sync_job,task_metadata)
        task_logger.info("Succeed to process the task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"]))
    except:
        message = traceback.format_exc()
        task_logger.error("Failed to Process the task ({0} - {1} {2}).{3}".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"],message))

def execute_prepare_task(sync_job,task_metadata,task_logger):
    """
    execute the prepare task, based on tasks_metadata
    """
    task_name = taskname(sync_job,task_metadata)
    task_status = sync_job['status'].get_task_status(task_metadata[TASK_TYPE_INDEX])

    task_logger.info("Begin to process the prepare task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"]))
    sync_job['status'].last_process_time = now()
    task_status.last_process_time = now()
    try:
        task_metadata[TASK_HANDLER_INDEX](sync_job,task_metadata,task_status)
        if not task_status.get_message("message"):
            task_status.set_message("message","succeed")
        task_status.succeed()
        task_logger.info("Succeed to process the task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"]))
    except:
        task_status.failed()
        message = traceback.format_exc()
        task_status.set_message("message",message)
        task_logger.error("Failed to Process the task ({0} - {1} {2}).{3}".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"],message))

def execute_task(sync_job,task_metadata,task_logger):
    """
    execute the task, based on tasks_metadata
    Return True if succeed otherwise return False
    """
    task_name = taskname(sync_job,task_metadata)
    task_status = sync_job['status'].get_task_status(task_metadata[TASK_TYPE_INDEX])

    if task_status.is_succeed: 
        #this task has been executed successfully
        return True

    if sync_job['status'].is_failed:
        #some proceding task are failed,so can't execute this task
        if task_status.shared:
            #this task is shared, but this task can't executed for this job, change the task's status object to a private status object
            from slave_sync_status import SlaveSyncTaskStatus
            sync_job['status'].set_task_status(task_metadata[TASK_TYPE_INDEX],SlaveSyncTaskStatus())
        return False

    task_logger.info("Begin to process the {3}task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"],"shared " if task_status.shared else ""))
    sync_job['status'].last_process_time = now()
    task_status.last_process_time = now()
    try:
        task_status.del_message("message")
        task_metadata[TASK_HANDLER_INDEX](sync_job,task_metadata,task_status)
        if not task_status.get_message("message"):
            task_status.set_message("message","succeed")
        task_status.succeed()
        task_logger.info("Succeed to process the {3}task ({0} - {1} {2}).".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"],"shared " if task_status.shared else ""))
        return True
    except:
        task_status.failed()
        message = traceback.format_exc()
        task_status.set_message("message",message)
        task_logger.error("Failed to Process the {4}task ({0} - {1} {2}).{3}".format(task_metadata[TASK_TYPE_INDEX],task_name,sync_job["job_file"],message,"shared " if task_status.shared else ""))
        return False

