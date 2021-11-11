import logging
import traceback
import requests
import subprocess
import os
import sys
import json
from geoserver.support import JDBCVirtualTable,JDBCVirtualTableGeometry

import geoserver_catalog_extension

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

def geoserver_style_name(sync_job,style_name):
    if not style_name:
        return None
    elif style_name == "builtin":
        return sync_job['name']
    else:
        return "{}.{}".format(sync_job['name'],style_name)

def get_datastore(gs,sync_job):
    d_gs = gs.get_store(store_name(sync_job))

    if not d_gs:
        raise Exception("Datastore ({0}) does not exist".format(store_name(sync_job)))

    return d_gs

def get_feature(gs,sync_job):
    l_gs = gs.get_layer(task_feature_name(sync_job))
    if not l_gs:
        raise Exception("Feature ({0}) does not exist".format(task_feature_name(sync_job)))

    return l_gs

def _create_datastore(sync_job,task_metadata,task_status,gs,stage=None):
    name = store_name(sync_job)
    try:
        d_gs = gs.get_store(name,sync_job['workspace'])
    except:
        d_gs = gs.create_datastore(name, sync_job['workspace'])

    d_gs.connection_parameters = dict(settings.GEOSERVER_PGSQL_CONNECTION_DEFAULTS)
    d_gs.enabled = True

    for k in d_gs.connection_parameters.iterkeys():
        if k in sync_job:
            d_gs.connection_parameters[k] = str(sync_job[k])

    d_gs.connection_parameters["namespace"] = settings.GEOSERVER_WORKSPACE_NAMESPACE.format(sync_job['workspace'])

    if "geoserver_setting" in sync_job:
        for k in d_gs.connection_parameters.iterkeys():
            if k in sync_job["geoserver_setting"]:
                d_gs.connection_parameters[k] = str(sync_job["geoserver_setting"][k])
    gs.save(d_gs)
    d_gs = gs.get_store(name)
    if not d_gs:
        raise Exception("Create data store for workspace({0}) in geoserver failed.".format(sync_job['workspace']))

def create_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_datastore)

def _delete_datastore(sync_job,task_metadata,task_status,gs,stage=None):
    name = store_name(sync_job)
    try:
        d_gs = gs.get_store(name)
    except:
        #datastore not exist
        return

    gs.delete(d_gs)

def delete_datastore(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_delete_datastore)

class Feature(object):
    def __init__(self,layer,href):
        self.layer = layer
        self.href = href

def _delete_feature(sync_job,task_metadata,task_status,gs,rest_url,stage=None):
    feature_name = "{}:{}".format(sync_job['workspace'], sync_job['name'])
    l_gs = gs.get_layer(feature_name)
    styles = {}
    feature_exist = False
    #try to find associated feature's private styles
    if l_gs:
        #delete alternate styles
        feature_exist = True
        for s_gs in l_gs.styles or {}:
            if s_gs.name.startswith(sync_job['name']):
                #the alternate style is only used by this feature, save it for delete.
                styles[s_gs.name] = s_gs

        #try to delete default style
        if l_gs.default_style and l_gs.default_style.name.startswith(sync_job['name']):
            #has default style and default style is only used by this feature, save it for delete it.
            styles[l_gs.default_style.name] = l_gs.default_style

    #try to find feature's private styles but failed to attach to the feature
    for name,style in sync_job["styles"].iteritems():
        style_name = geoserver_style_name(sync_job,name)
        if style_name in styles:
            continue
        s_gs = gs.get_style(name=style_name, workspace=sync_job["workspace"])
        if s_gs:
            styles[style_name] = s_gs

    #delete the feature
    if l_gs:
        #delete layer
        gs.delete(l_gs)

    try:
        #delete feature
        url = "{}/workspaces/{}/datastores/{}/featuretypes/{}.xml".format(rest_url,sync_job['workspace'],store_name(sync_job),sync_job['name'])
        gs.delete(Feature(l_gs,url))
    except:
        pass


    #delete the styles
    for style in styles.itervalues():
        gs.delete(style)

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

def _create_feature(sync_job,task_metadata,task_status,gs,stage=None):
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

def _create_style(sync_job,task_metadata,task_status,gs,stage=None):
    """
    This is not a critical task. 
    """
    default_style = None
    created_styles = []
    style_name = None
    messages = []
    default_style_name = geoserver_style_name(sync_job,sync_job.get('default_style',None))
    #create styles
    for name,style in sync_job["styles"].iteritems():
        style_name = geoserver_style_name(sync_job,name)

        try:
            with open(style['local_file']) as f:
                sld_data = f.read()

            # kludge to match for SLD 1.1
            style_format = "sld10"
            if "version=\"1.1.0\"" in sld_data:
                style_format = "sld11"

            gs.create_style(name=style_name, data=sld_data, workspace=sync_job['workspace'], style_format=style_format)
            s_gs = gs.get_style(name=style_name, workspace=sync_job['workspace'])
            if s_gs.name == default_style_name:
                default_style = s_gs
            else:
                created_styles.append(s_gs)

        except:
            message = traceback.format_exc()
            logger.error("Create style({}) failed ({}) failed. {}".format(style_name,task_style_name(sync_job),message))
            messages.append("Failed to create style ({}). {}".format(style_name,message))
    
    if not default_style and created_styles :
        #default style is not set, set the default style to the first created styles.
        default_style = created_styles[0]
        del created_styles[0]

    if default_style:
        if created_styles:
            messages.append("Succeed to create styles ({}, {}).".format(default_style.name, ", ".join([s.name for s in created_styles])))
        else:
            messages.append("Succeed to create style ({}).".format(default_style.name))
    
        #try to set feature's styles
        try:
            feature = get_feature(gs,sync_job)
            feature.default_style = default_style
            if created_styles:
                feature.styles = created_styles

            gs.save(feature)
            messages.append("Succeed to set default style ({}).".format(default_style.name))
            if created_styles:
                messages.append("Succeed to set alternative styles ({}).".format(", ".join([s.name for s in created_styles])))
        except:
            message = traceback.format_exc()
            logger.error("Failed to set default style({}) and alternative styles ({}).{}".format(default_style.name, ", ".join([s.name for s in created_styles]),message))
            messages.append("Failed to set default style ({}) and alternative styles ({}). {}".format(default_style.name, ", ".join([s.name for s in created_styles]),message))
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
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_update_access_rules,lambda index:(settings.GEOSERVER_DATA_DIR[index],))

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

def _create_workspace(sync_job,task_metadata,task_status,gs,stage=None):
    try:
        w_gs = gs.get_workspace(sync_job['workspace'])
    except:
        w_gs = None

    if not w_gs:
        w_gs = gs.create_workspace(sync_job['workspace'], settings.GEOSERVER_WORKSPACE_NAMESPACE.format(sync_job['workspace']))

def create_workspace(sync_job,task_metadata,task_status):
    settings.apply_to_geoservers(sync_job,task_metadata,task_status,_create_workspace)

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
