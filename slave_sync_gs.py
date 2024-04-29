import logging
import traceback
import requests
import subprocess
import os
import sys
import json
from . import geoserver_rests as gs

import slave_sync_env as settings

from slave_sync_task import (
    update_feature_job,update_feature_metadata_job,gs_feature_task_filter,remove_feature_job,gs_style_task_filter,
    update_access_rules_job,update_wmsstore_job,gs_task_filter,update_layergroup_job,
    update_livestore_job,update_livelayer_job,remove_livestore_job,remove_livelayer_job,
    remove_wmsstore_job,update_wmslayer_job,remove_wmslayer_job,remove_layergroup_job,empty_gwc_layer_job,empty_gwc_livelayer_job,
    empty_gwc_group_job,empty_gwc_feature_job,update_workspace_job
)

logger = logging.getLogger(__name__)

task_workspace_name = lambda t: t["workspace"]
task_feature_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])
task_style_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

def store_name(sync_job):
    if sync_job["job_type"] == "live_store":
        return sync_job["name"]
    elif sync_job["job_type"] == "live_layer":
        return sync_job["datastore"]
    elif sync_job["job_type"] == "feature":
        return settings.GEOSERVER_DATASTORE_NAMESPACE.format(sync_job['workspace'])
    else:
        raise Exception("{} not support".format(sync_job["job_type"]))

task_store_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],store_name(sync_job))

def geoserver_stylename(sync_job,style_name):
    if not style_name:
        return None
    elif style_name == "builtin":
        return sync_job['name']
    else:
        return "{}.{}".format(sync_job['name'],style_name)

def _create_workspace(sync_job,task_metadata,task_status,rest_url,username,password,stage=None):
    workspace = sync_job['workspace']
    gs.create_workspace(rest_url,workspace,username,password)

def create_workspace(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_workspace)

def _create_datastore(sync_job,task_metadata,task_status,rest_url,username,password,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = store_name(sync_job)
    gs.create_datastore(rest_url,workspace,storename,username,password)

def create_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_datastore)

def _delete_datastore(sync_job,task_metadata,task_status,rest_url,username,password,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = store_name(sync_job)
    gs.delete_datastore(rest_url,workspace,storename,username,password)

def delete_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_delete_datastore)

def _delete_feature(sync_job,task_metadata,task_status,gs,rest_url,username,password,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = store_name(sync_job)
    layername = sync_job['name']

    styles = []
    feature_exist = False
    if gs.has_featuretype(rest_url,workspace,storename,layername,username,password):
        #find the related styles first
        for default_style,other_styles in gs.get_layer_styles(rest_url,workspace,layername,username,password):
            if default_style:
                if default_style.startswith(layername):
                    #the alternate style is only used by this feature, save it for delete.
                    styles.append(d["name"])
            if other_styles:
                for stylename in other_styles:
                    if stylename.startswith(layername):
                        #the alternate style is only used by this feature, save it for delete.
                        styles.append(stylename)
        #delete the feature
        gs.delete_featuretype(rest_url,workspace,storename,layername,username,password)
        feature_exist = True

    #try to find feature's private styles but failed to attach to the feature
    for name,style in sync_job["styles"].iteritems():
        stylename = geoserver_stylename(sync_job,name)
        if stylename in styles:
            continue
        if gs.has_style(workspace,stylename):
            styles.append(stylename)

    #delete the styles
    for stylename in styles:
        gs.delete_style(rest_url,workspace,stylename,username,password)

    if feature_exist:
        if  styles:
            task_status.set_message("message",os.linesep.join([
                "Succeed to delete feature ({})".format(feature_name),
                "Succeed to delete private styles ({}).".format(", ".join([name for name in styles.iterkeys()]))
                ]),stage=stage)
        else:
            task_status.set_message("message","Succeed to delete feature ({}).".format(feature_name),stage=stage)
    else:
        if styles:
            task_status.set_message("message",os.linesep.join([
                "Feature ({}) doesn't exist.".format(feature_name),
                "Succeed to delete private styles ({}).".format(", ".join([name for name in styles.iterkeys()]))
                ]),stage=stage)
        else:
            task_status.set_message("message","Feature ({}) doesn't exist.".format(feature_name),stage=stage)

def delete_feature(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_delete_feature,lambda index:(settings.gs[index],settings.GEOSERVER_REST_URL[index]))

def _create_feature(sync_job,task_metadata,task_status,gs,rest_url,username,password,stage=None):
    """
    This is not a critical task. 
    """
    crs = sync_job.get("crs",None)
    if not crs and "datasource" not in sync_job:
        # try and fetch the layer's CRS from PostGIS
        if "spatial_column" in sync_job:
            getcrs_cmd = ["psql", "-w", "-h", settings.GEOSERVER_PGSQL_HOST, "-p", settings.GEOSERVER_PGSQL_PORT, "-d", settings.GEOSERVER_PGSQL_DATABASE, "-U", settings.GEOSERVER_PGSQL_USERNAME, "-A", "-t", "-c", "SELECT srid FROM public.geometry_columns WHERE f_table_schema='{0}' AND f_table_name='{1}' AND f_geometry_column='{2}';".format(sync_job["schema"], sync_job["name"],sync_job["spatial_column"])]
        else:
            getcrs_cmd = ["psql", "-w", "-h", settings.GEOSERVER_PGSQL_HOST, "-p", settings.GEOSERVER_PGSQL_PORT, "-d", settings.GEOSERVER_PGSQL_DATABASE, "-U", settings.GEOSERVER_PGSQL_USERNAME, "-A", "-t", "-c", "SELECT public.ST_SRID(wkb_geometry) FROM {}.{} LIMIT 1;".format(sync_job["schema"], sync_job["name"])]
        getcrs = subprocess.Popen(getcrs_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=settings.env)
        getcrs_output = getcrs.communicate()
        if not getcrs_output[0]:
            crs = settings.GEOSERVER_DEFAULT_CRS
            message = 'No CRS found for {}.{}, using default of {}'.format(sync_job["schema"], sync_job["name"], crs)
            task_status.set_message("message",message,stage=stage)
            logger.info(message)
        else:
            srid = getcrs_output[0].decode('utf-8').strip()
            if len(srid) == 6 and srid.startswith('90'):
                crs = settings.GEOSERVER_DEFAULT_CRS
                message = 'Layer {}.{} has the non-standard SRID {}! Check the Borg Collector definition for this input and force a standard CRS if necessary. For now, the layer will be published with default CRS {}'.format(sync_job["schema"], sync_job["name"], srid, crs)
                logger.warn(message)
                task_status.set_message("message",message,stage=stage)
            else:
                crs = 'EPSG:{}'.format(srid)
                message = 'Found CRS for {}.{}: {}'.format(sync_job["schema"], sync_job["name"], crs)
                logger.info(message)
                task_status.set_message("message",message,stage=stage)

    bbox = None
    if (sync_job.get('override_bbox',False)):
        bbox = sync_job["bbox"]
        bbox = (repr(bbox[0]),repr(bbox[2]),repr(bbox[1]),repr(bbox[3]),crs)



    if sync_job.get('viewsql'):
        gs.publish_featuretype(sync_job['name'],get_datastore(gs,sync_job),crs,
            keywords = (sync_job.get('keywords',None) or []) + (sync_job.get('applications',None) or []), 
            title=sync_job.get('title', None), 
            abstract=sync_job.get('abstract', None),
            jdbc_virtual_table=JDBCVirtualTable(sync_job['name'],sync_job.get('viewsql'),'false',JDBCVirtualTableGeometry(sync_job["spatial_column"],sync_job["spatial_type"],crs[5:])),
            nativeBoundingBox=bbox,
            latLonBoundingBox=bbox
        )
    else:
        gs.publish_featuretype(sync_job['name'],get_datastore(gs,sync_job),crs,
            keywords = (sync_job.get('keywords',None) or []) + (sync_job.get('applications',None) or []), 
            title=sync_job.get('title', None), 
            abstract=sync_job.get('abstract', None),
            nativeName=sync_job.get('table',None),
            nativeBoundingBox=bbox,
            latLonBoundingBox=bbox
        )

    name = task_feature_name(sync_job)
    l_gs = gs.get_layer(name)
    if not l_gs:
        raise Exception("Layer({0}) not registering.".format(name))

def create_feature(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_feature)

def _create_style(sync_job,task_metadata,task_status,gs,rest_url,username,password,stage=None):
    """
    This is not a critical task. 
    """
    workspace = sync_job['workspace']
    created_styles = []
    #create styles
    for name,style in sync_job["styles"].iteritems():
        stylename = geoserver_stylename(sync_job,name)

        try:
            with open(style['local_file']) as f:
                slddata = f.read()

            sldversion = "1.1.0" if "version=\"1.1.0\"" in slddata else "1.0.0"

            gs.update_style(rest_url,workspace,stylename,sldversion,slddata,username,password):
            s_gs = gs.get_style(name=style_name, workspace=sync_job['workspace'])
            created_styles.append(s_gs)
        except:
            message = traceback.format_exc()
            logger.error("Create style({}) failed ({}) failed. {}".format(style_name,task_style_name(sync_job),message))
            messages.append("Failed to create style ({}). {}".format(style_name,message))
    
    if created_styles:
        messages.append("Succeed to create styles ({}).".format(" , ".join([s.name for s in created_styles])))
    else:
        messages.append("No styles are reauired to create")

    #set messages
    task_status.set_message("message",os.linesep.join(messages),stage=stage)

def create_style(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_style)

def _update_access_rules(sync_job,task_metadata,task_status,data_dir,stage=None):
    with open(os.path.join(data_dir,"security","layers.properties"),"wb") as access_file:
        access_file.write(sync_job["job_file_content"])

def update_access_rules(sync_job,task_metadata,task_status):
    if settings.GEOSERVER_SHARING_DATA_DIR:
        settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_access_rules,lambda index:(settings.GEOSERVER_DATA_DIR[index],),start=0,end=1)
    else:
        settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_access_rules,lambda index:(settings.GEOSERVER_DATA_DIR[index],),start=0,end=len(settings.GEOSERVER_URL))

def _reload_geoserver(sync_job,task_metadata,task_status,gs,stage=None):
    """
    reload geoserver setting
    always succeed, even failed.
    """
    try:
        gs.reload()
    except:
        logger.error("Reload geoserver setting failed".format(traceback.format_exc()))

def reload_geoserver(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_reload_geoserver)

def reload_dependent_geoservers(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_reload_geoserver,start=1)

tasks_metadata = [
                    ("create_datastore", update_livestore_job, gs_feature_task_filter      , task_store_name  , create_datastore),
                    ("create_datastore", update_feature_job, gs_feature_task_filter      , task_store_name  , create_datastore),
                    ("create_datastore", update_feature_metadata_job, gs_feature_task_filter      , task_store_name  , create_datastore),

                    ("delete_datastore", remove_livestore_job, gs_feature_task_filter      , task_store_name  , delete_datastore),

                    ("delete_feature"  , update_feature_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , update_livelayer_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , update_feature_metadata_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , remove_feature_job, gs_feature_task_filter      , task_feature_name, delete_feature),
                    ("delete_feature"  , remove_livelayer_job, gs_feature_task_filter      , task_feature_name, delete_feature),

                    ("create_feature"  , update_livelayer_job, gs_feature_task_filter      , task_feature_name, create_feature),
                    ("create_feature"  , update_feature_job, gs_feature_task_filter      , task_feature_name, create_feature),
                    ("create_feature"  , update_feature_metadata_job, gs_feature_task_filter      , task_feature_name, create_feature),

                    ("create_style"  , update_livelayer_job, gs_feature_task_filter      , task_feature_name, create_style),
                    ("create_style"    , update_feature_job, gs_style_task_filter, task_style_name  , create_style),
                    ("create_style"    , update_feature_metadata_job, gs_style_task_filter, task_style_name  , create_style),

                    ("update_access_rules", update_access_rules_job, None, "update_access_rules", update_access_rules),

                    ("create_workspace"   , update_wmsstore_job    , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_livestore_job   , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_layergroup_job  , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_job     , gs_feature_task_filter , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_metadata_job     , gs_feature_task_filter , task_workspace_name  , create_workspace),

                    ("reload_geoserver"   , update_access_rules_job, None, "reload_geoserver"   , reload_geoserver),
]
if settings.GEOSERVER_SHARING_DATA_DIR and len(settings.GEOSERVER_URL) > 1:
    for job,task_filter in (
        (update_access_rules_job,None),
        (update_wmsstore_job,gs_task_filter),
        (remove_wmsstore_job,gs_task_filter),
        (update_wmslayer_job,gs_feature_task_filter),
        (remove_wmslayer_job,gs_feature_task_filter),
        (update_livestore_job,gs_task_filter),
        (remove_livestore_job,gs_task_filter),
        (update_livelayer_job,gs_feature_task_filter),
        (remove_livelayer_job,gs_feature_task_filter),
        (update_layergroup_job,gs_task_filter),
        (remove_layergroup_job,gs_task_filter),
        (empty_gwc_layer_job,gs_feature_task_filter),
        (empty_gwc_livelayer_job,gs_feature_task_filter),
        (empty_gwc_group_job,gs_feature_task_filter),
        (update_feature_job,gs_feature_task_filter),
        (remove_feature_job,gs_feature_task_filter),
        (update_feature_metadata_job,gs_feature_task_filter),
        (empty_gwc_feature_job,gs_feature_task_filter),
        (update_workspace_job,gs_feature_task_filter)
    ):
        tasks_metadata.append(("reload_dependent_geoservers",job,task_filter,'reload_dependent_geoservers',reload_dependent_geoservers))
