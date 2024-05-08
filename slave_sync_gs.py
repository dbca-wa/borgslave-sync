import logging
import traceback
import requests
import subprocess
import collections
import os
import sys
import json
import geoserver_restapi as gs

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

def get_storename(sync_job):
    if sync_job["job_type"] == "live_store":
        return sync_job["name"]
    elif sync_job["job_type"] == "live_layer":
        return sync_job["datastore"]
    elif sync_job["job_type"] == "feature":
        return settings.GEOSERVER_DATASTORE_NAMESPACE.format(sync_job['workspace'])
    else:
        raise Exception("{} not support".format(sync_job["job_type"]))

task_store_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],get_storename(sync_job))

def get_stylename(sync_job,style_name):
    if not style_name:
        return None
    elif style_name == "builtin":
        return sync_job['name']
    else:
        return "{}_{}".format(sync_job['name'],style_name)

def _create_workspace(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = sync_job['workspace']
    if gs.has_workspace(geoserver_url,username,password,workspace):
        #workspace already exists
        return

    gs.create_workspace(geoserver_url,username,password,workspace)

def create_workspace(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_workspace)

def _create_datastore(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    if gs.has_datastore(geoserver_url,username,password,workspace,storename):
        #already exists
        return
    if "geoserver_setting" in sync_job:
        parameters = collections.ChainMap(sync_job["geoserver_setting"],sync_job)
    else:
        parameters = sync_job

    if "SSL mode" not in parameters:
        if sync_job["job_type"] == "feature":
            sync_job["SSL mode"] = None
        elif any(parameters.get("host","").startswith(s) for s in ["az-aks-","az-k3s-"]):
            sync_job["SSL mode"] = None
        else:
            sync_job["SSL mode"] = "PREFER"


    if "geoserver_setting" in sync_job:
        gs.update_datastore(geoserver_url,username,password,workspace,storename,collections.ChainMap(sync_job["geoserver_setting"],sync_job),create=True)
    else:
        gs.update_datastore(geoserver_url,username,password,workspace,storename,sync_job,create=True)

def create_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_datastore)

def _update_datastore(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    if "geoserver_setting" in sync_job:
        gs.update_datastore(geoserver_url,username,password,workspace,storename,collections.ChainMap(sync_job,sync_job["geoserver_setting"]))
    else:
        gs.update_datastore(geoserver_url,username,password,workspace,storename,sync_job)

def update_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_datastore)

def _delete_datastore(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    gs.delete_datastore(geoserver_url,username,password,workspace,storename)

def delete_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_delete_datastore)

def _delete_feature(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    layername = sync_job['name']

    styles = []
    feature_exist = False
    if gs.has_featuretype(geoserver_url,username,password,workspace,storename,layername):
        #find the related styles first
        default_style,other_styles = gs.get_layer_styles(geoserver_url,username,password,workspace,layername)
        if default_style:
            if default_style.startswith(layername):
                #the alternate style is only used by this feature, save it for delete.
                styles.append(default_style)
        if other_styles:
            for stylename in other_styles:
                if stylename.startswith(layername):
                    #the alternate style is only used by this feature, save it for delete.
                    styles.append(stylename)
        #delete the feature
        gs.delete_featuretype(geoserver_url,username,password,workspace,storename,layername)
        feature_exist = True

    #try to find feature's private styles but failed to attach to the feature
    for name,style in sync_job["styles"].items():
        stylename = get_stylename(sync_job,name)
        if stylename in styles:
            continue
        if gs.has_style(geoserver_url,username,password,workspace,stylename):
            styles.append(stylename)

    #delete the styles
    for stylename in styles:
        gs.delete_style(geoserver_url,username,password,workspace,stylename)

    if feature_exist:
        if  styles:
            task_status.set_message("message",os.linesep.join([
                "Succeed to delete feature ({}:{})".format(workspace,layername),
                "Succeed to delete private styles ({}).".format(", ".join([name for name in styles]))
                ]),stage=stage)
        else:
            task_status.set_message("message","Succeed to delete feature ({}:{}).".format(workspace,layername),stage=stage)
    else:
        if styles:
            task_status.set_message("message",os.linesep.join([
                "Feature ({}:{}) doesn't exist.".format(workspace,layername),
                "Succeed to delete private styles ({}).".format(", ".join([name for name in styles]))
                ]),stage=stage)
        else:
            task_status.set_message("message","Feature ({}:{}) doesn't exist.".format(workspace,layername),stage=stage)

def delete_feature(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_delete_feature)

def _create_feature(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    This is not a critical task. 
    """
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    layername = sync_job['name']

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

    extra_data = {}
    if (sync_job.get('override_bbox',False)):
        bbox = sync_job["bbox"]
        bbox = (repr(bbox[0]),repr(bbox[2]),repr(bbox[1]),repr(bbox[3]),sync_job["crs"])
        extra_data["nativeBoundingBox"] = bbox
        extra_data["latLonBoundingBox"] = bbox
    else:
        extra_data["nativeBoundingBox"] = None
        extra_data["latLonBoundingBox"] = None

    extra_data["srs"] = crs

    if sync_job.get('keywords')  and sync_job.get('applications'):
        keywords = sync_job["keywords"] + sync_job["applications"]
    elif sync_job.get('keywords') :
        keywords = sync_job["keywords"]
    elif sync_job.get('applications'):
        keywords = sync_job["applications"]
    else:
        keywords = []

    extra_data["keywords"] = keywords
    #create the featuretype
    gs.publish_featuretype(geoserver_url,username,password,workspace,storename,layername,collections.ChainMap(extra_data,sync_job))

def create_feature(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_feature)

def _set_feature_styles(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    workspace = task_workspace_name(sync_job)
    storename = get_storename(sync_job)
    layername = sync_job['name']

    #set layer styles
    default_stylename = get_stylename(sync_job,sync_job.get('default_style',None))
    if default_stylename:
        if not gs.has_style(geoserver_url,username,password,workspace,default_stylename):
            default_stylename = None
    stylenames = []
    for name in sync_job["styles"].keys():
        stylename = get_stylename(sync_job,name)
        if not gs.has_style(geoserver_url,username,password,workspace,stylename):
            continue
        if not default_stylename:
            default_stylename = stylename
        elif default_stylename != stylename:
            stylenames.append(stylename)

    gs.set_layer_styles(geoserver_url,username,password,workspace,layername,default_stylename,stylenames)



def set_feature_styles(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_set_feature_styles)

def _create_style(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    This is not a critical task. 
    """
    workspace = sync_job['workspace']
    created_styles = []
    messages = []
    #create styles
    for name,style in sync_job["styles"].items():
        stylename = get_stylename(sync_job,name)

        with open(style['local_file']) as f:
            slddata = f.read()

        sldversion = "1.1.0" if "version=\"1.1.0\"" in slddata else "1.0.0"

        gs.update_style(geoserver_url,username,password,workspace,stylename,sldversion,slddata)
        created_styles.append(task_style_name(sync_job))
    
    if created_styles:
        messages.append("Succeed to create styles ({}).".format(" , ".join(created_styles)))
    else:
        messages.append("No styles has been created")

    #set messages
    task_status.set_message("message",os.linesep.join(messages),stage=stage)

def create_style(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_style)

def _update_access_rules(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    layer_access_rules = {}
    catalogue_mode = "CHALLENGE"
    for line in re.split("\n|\r\n",sync_job["job_file_content"]):
        line = line.strip()
        if not line:
            continue
        data = [d.strip() for d in line.split("=",1)]
        if len(data) != 2:
            continue
        if data[0].lower() == "mode":
            catalogue_mode = data[1]
        else:
            layer_access_rules[data[0]] = data[1]

    gs.set_catalogue_mode(geoserver_url,catalogue_mode,username,password)
    gs.update_layer_access_rules(geoserver_url,layer_access_rules,username,password)

def update_access_rules(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_access_rules)

def _reload_geoserver(geoserver_url,username,password,sync_job,task_metadata,task_status,stage=None):
    """
    reload geoserver setting
    always succeed, even failed.
    """
    gs.reload(geoserver_url,username,password)

def reload_geoserver(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_reload_geoserver)

tasks_metadata = [
                    ("create_datastore", update_livestore_job, gs_feature_task_filter      , task_store_name  , update_datastore),
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

                    ("set_feature_styles"  , update_livelayer_job, gs_feature_task_filter      , task_feature_name, set_feature_styles),
                    ("set_feature_styles"  , update_feature_job, gs_feature_task_filter      , task_feature_name, set_feature_styles),
                    ("set_feature_styles"  , update_feature_metadata_job, gs_feature_task_filter      , task_feature_name, set_feature_styles),

                    ("create_style"  , update_livelayer_job, gs_feature_task_filter      , task_feature_name, create_style),
                    ("create_style"    , update_feature_job, gs_style_task_filter, task_style_name  , create_style),
                    ("create_style"    , update_feature_metadata_job, gs_style_task_filter, task_style_name  , create_style),

                    ("update_access_rules", update_access_rules_job, None, "update_access_rules", update_access_rules),

                    ("create_workspace"   , update_wmsstore_job    , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_livestore_job   , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_layergroup_job  , gs_task_filter         , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_job     , gs_feature_task_filter , task_workspace_name  , create_workspace),
                    ("create_workspace"   , update_feature_metadata_job     , gs_feature_task_filter , task_workspace_name  , create_workspace)
]
