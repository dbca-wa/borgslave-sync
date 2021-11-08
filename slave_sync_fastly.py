import logging
import traceback
import requests

from . import slave_sync_env as env

from slave_sync_task import (
    update_wmslayer_job,remove_wmslayer_job,empty_gwc_layer_job,
    update_livelayer_job,remove_livelayer_job,empty_gwc_livelayer_job,
    update_layergroup_job,remove_layergroup_job,empty_gwc_group_job,
    update_feature_job,update_feature_metadata_job,remove_feature_job,empty_gwc_feature_job,
    gs_feature_task_filter,gs_task_filter
)

logger = logging.getLogger(__name__)

task_feature_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

PURGE_URL = "/service/{0}/purge/layer-{{0}}".format(env.FASTLY_SERVICEID)
def purge_fastly_cache(sync_job,task_metadata,task_status):
    layer_name = task_feature_name(sync_job)
    purge_url = PURGE_URL.format(layer_name)

    try:
        resp = requests.post(purge_url, headers={'Accept':'application/json','Fastly-Soft-Purge':env.FASTLY_SOFT_PURGE,'Fastly-Key':env.FASTLY_API_TOKEN})
        resp.raise_for_status()
    except Exception as ex:
        raise Exception("Failed to purge fastly cache via url({}).{}".format(purge_url,str(ex)))


tasks_metadata = []
]

if env.FASTLY_SERVICEID and env.FASTLY_API_TOKEN:
    for job,task_filter in (
        (update_wmslayer_job,gs_feature_task_filter),
        (remove_wmslayer_job,gs_feature_task_filter),
        (empty_gwc_layer_job,gs_feature_task_filter),

        (update_livelayer_job,gs_feature_task_filter),
        (remove_livelayer_job,gs_feature_task_filter),
        (empty_gwc_livelayer_job,gs_feature_task_filter),

        (update_layergroup_job,gs_task_filter),
        (remove_layergroup_job,gs_task_filter),
        (empty_gwc_group_job,gs_feature_task_filter),

        (update_feature_job,gs_feature_task_filter),
        (remove_feature_job,gs_feature_task_filter),
        (update_feature_metadata_job,gs_feature_task_filter),
        (empty_gwc_feature_job,gs_feature_task_filter),

    ):
        tasks_metadata.append(("purge_fastly_cache",job,task_filter,task_feature_name,purge_fastly_cache))
