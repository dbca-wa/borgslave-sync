import logging
import traceback
import subprocess
import os

import geoserver_catalog_extension
from slave_sync_env import (
    GEOSERVER_DATASTORE_NAMESPACE,GEOSERVER_PGSQL_CONNECTION_DEFAULTS,GEOSERVER_WORKSPACE_NAMESPACE,GEOSERVER_DEFAULT_CRS,GEOSERVER_DATA_DIR,
    GEOSERVER_PGSQL_HOST, GEOSERVER_PGSQL_DATABASE, GEOSERVER_PGSQL_USERNAME,
    CACHE_PATH,
    gs,env
)
from slave_sync_task import (
    update_feature_job,update_feature_metadata_job,gs_feature_task_filter,remove_feature_job,gs_style_task_filter,
    update_access_rules_job,update_wmsstore_job,gs_task_filter,update_layergroup_job
)

logger = logging.getLogger(__name__)

store_name = lambda sync_job: GEOSERVER_DATASTORE_NAMESPACE.format(sync_job['workspace'])

task_workspace_name = lambda t: t["workspace"]
task_store_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],GEOSERVER_DATASTORE_NAMESPACE.format(sync_job['workspace']))
task_feature_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])
task_style_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

def get_datastore(sync_job):
    d_gs = gs.get_store(store_name(sync_job))

    if not d_gs:
        raise Exception("Datastore ({0}) does not exist".format(store_name(sync_job)))

    return d_gs

def get_feature(sync_job):
    l_gs = gs.get_layer(task_feature_name(sync_job))
    if not l_gs:
        raise Exception("Feature ({0}) does not exist".format(task_feature_name(sync_job)))

    return l_gs


def create_datastore(sync_job,task_metadata,task_status):
    name = store_name(sync_job)
    try:
        d_gs = gs.get_store(name)
    except:
        d_gs = gs.create_datastore(name, sync_job['workspace'])

    d_gs.connection_parameters = dict(GEOSERVER_PGSQL_CONNECTION_DEFAULTS)
    d_gs.connection_parameters.update({
        "schema": sync_job["schema"],
        "namespace": GEOSERVER_WORKSPACE_NAMESPACE.format(sync_job['workspace']),
    })
    gs.save(d_gs)
    d_gs = gs.get_store(name)
    if not d_gs:
        raise Exception("Create data store for workspace({0}) in geoserver failed.".format(sync_job['workspace']))

def delete_feature(sync_job,task_metadata,task_status):
    l_gs = gs.get_layer("{}:{}".format(sync_job['workspace'], sync_job['name']))
    if l_gs:
        gs.delete(l_gs)

def delete_style(sync_job,task_metadata,task_status):
    s_gs = gs.get_style(name=sync_job["name"], workspace=sync_job["workspace"])
    if s_gs:
        gs.delete(s_gs)

def create_feature(sync_job,task_metadata,task_status):
    """
    This is not a critical task. 
    """
    # try and fetch the layer's CRS from PostGIS
    getcrs_cmd = ["psql", "-w", "-h", GEOSERVER_PGSQL_HOST, "-d", GEOSERVER_PGSQL_DATABASE, "-U", GEOSERVER_PGSQL_USERNAME, "-A", "-t", "-c", "SELECT public.ST_SRID(wkb_geometry) FROM {}.{} LIMIT 1;".format(sync_job["schema"], sync_job["name"])]
    getcrs = subprocess.Popen(getcrs_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    getcrs_output = getcrs.communicate()
    if not getcrs_output[0]:
        crs = GEOSERVER_DEFAULT_CRS
        message = 'No CRS found for {}.{}, using default of {}'.format(sync_job["schema"], sync_job["name"], crs)
        task_status.set_message("message",message)
        logger.info(message)
    else:
        srid = getcrs_output[0].decode('utf-8').strip()
        crs = 'EPSG:{}'.format(srid)
        if len(srid) == 6 and srid.startswith('90'):
            crs = GEOSERVER_DEFAULT_CRS
            message = 'Layer {}.{} has the non-standard SRID {}! Check the Borg Collector definition for this input and force a standard CRS if necessary. For now, the layer will be published with default CRS {}'.format(sync_job["schema"], sync_job["name"], srid, crs)
            logger.warn(message)
            task_status.set_message("message",message)
        else:
            message = 'Found CRS for {}.{}: {}'.format(sync_job["schema"], sync_job["name"], crs)
            logger.info(message)
            task_status.set_message("message",message)

    gs.publish_featuretype(sync_job['name'],get_datastore(sync_job),crs,keywords = sync_job.get('applications',None), title=sync_job.get('title', None), abstract=sync_job.get('abstract', None))
    name = task_feature_name(sync_job)
    l_gs = gs.get_layer(name)
    if not l_gs:
        gs.reload()
        gs.publish_featuretype(sync_job['name'],get_datastore(sync_job),GEOSERVER_DEFAULT_CRS,keywords = sync_job.get('applications',None), title=sync_job.get('title', None), abstract=sync_job.get('abstract', None))
        l_gs = gs.get_layer(name)
        if not l_gs:
            raise Exception("Layer({0}) not registering.".format(name))

def create_style(sync_job,task_metadata,task_status):
    """
    This is not a critical task. 
    """
    try:
        output_name = os.path.join(CACHE_PATH, "{}.sld".format(sync_job["name"]))
        with open(output_name) as f:
            sld_data = f.read()

        # kludge to match for SLD 1.1
        style_format = "sld10"
        if "version=\"1.1.0\"" in sld_data:
            style_format = "sld11"
        gs.create_style(name=sync_job['name'], data=sld_data, workspace=sync_job['workspace'], style_format=style_format)
        s_gs = gs.get_style(name=sync_job['name'], workspace=sync_job['workspace'])
        feature = get_feature(sync_job)
        feature.default_style = s_gs
        gs.save(feature)
    except:
        message = traceback.format_exc()
        task_status.set_message("message",message)
        logger.error("Create style failed ({0}) failed.{1}".format(task_style_name(sync_job),message))

def update_access_rules(sync_job,task_metadata,task_status):
    with open(os.path.join(GEOSERVER_DATA_DIR,"security","layers.properties"),"wb") as access_file:
        access_file.write(sync_job["job_file_content"])

def reload_geoserver(sync_job,task_metadata,task_status):
    """
    reload geoserver setting
    always succeed, even failed.
    """
    try:
        gs.reload()
    except:
        logger.error("Reload geoserver setting failed".format(traceback.format_exc()))

def create_workspace(sync_job,task_metadata,task_status):
    try:
        w_gs = gs.get_workspace(sync_job['workspace'])
    except:
        w_gs = None

    if not w_gs:
        w_gs = gs.create_workspace(sync_job['workspace'], GEOSERVER_WORKSPACE_NAMESPACE.format(sync_job['workspace']))

tasks_metadata = [
                    ("create_datastore", update_feature_job, gs_feature_task_filter      , task_store_name  , create_datastore),
                    ("create_datastore", update_feature_metadata_job, gs_feature_task_filter      , task_store_name  , create_datastore),

                    ("delete_feature"  , update_feature_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , update_feature_metadata_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , remove_feature_job, gs_feature_task_filter      , task_feature_name, delete_feature),

                    ("delete_style"    , update_feature_job, gs_style_task_filter, task_style_name  , delete_style),
                    ("delete_style"    , update_feature_metadata_job, gs_style_task_filter, task_style_name  , delete_style),

                    ("create_feature"  , update_feature_job, gs_feature_task_filter      , task_feature_name, create_feature),
                    ("create_feature"  , update_feature_metadata_job, gs_feature_task_filter      , task_feature_name, create_feature),

                    ("create_style"    , update_feature_job, gs_style_task_filter, task_style_name  , create_style),
                    ("create_style"    , update_feature_metadata_job, gs_style_task_filter, task_style_name  , create_style),

                    ("update_access_rules", update_access_rules_job, None, "update_access_rules", update_access_rules),

                    ("create_workspace"   , update_wmsstore_job    , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_layergroup_job  , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_job     , gs_feature_task_filter , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_metadata_job     , gs_feature_task_filter , task_workspace_name  , create_workspace),

                    ("geoserver_reload"   , update_access_rules_job, None, "reload_geoserver"   , reload_geoserver),
                    ("geoserver_reload"   , update_feature_job     , gs_feature_task_filter , "reload_geoserver"   , reload_geoserver),
                    ("geoserver_reload"   , update_feature_metadata_job     , gs_feature_task_filter , "reload_geoserver"   , reload_geoserver),
]

