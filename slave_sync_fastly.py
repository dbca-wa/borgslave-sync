import logging
import traceback
import requests

import slave_sync_env as settings

from slave_sync_task import (
    update_wmslayer_job,remove_wmslayer_job,empty_gwc_layer_job,
    update_livelayer_job,remove_livelayer_job,empty_gwc_livelayer_job,
    update_layergroup_job,remove_layergroup_job,empty_gwc_group_job,
    update_feature_job,update_feature_metadata_job,remove_feature_job,empty_gwc_feature_job,
    gs_feature_task_filter,gs_task_filter
)

logger = logging.getLogger(__name__)

task_feature_name = lambda sync_job: "{0}:{1}".format(sync_job['workspace'],sync_job['name'])

PURGE_URL = "/service/{0}/purge/{{0}}".format(settings.FASTLY_SERVICEID)
def purge_fastly_cache(sync_job,task_metadata,task_status):
    layer_name = task_feature_name(sync_job)

    purge_urls = []
    for k in settings.FASTLY_SURROGATE_KEY:
        purge_url = PURGE_URL.format(k).format(layer_name)
        try:
            resp = requests.post(purge_url, headers={'Accept':'application/json','Fastly-Soft-Purge':settings.FASTLY_SOFT_PURGE,'Fastly-Key':settings.FASTLY_API_TOKEN})
            resp.raise_for_status()
            purge_urls.append(puge_url)
        except Exception as ex:
            raise Exception("Failed to purge fastly cache via url({}).{}".format(purge_url,str(ex)))

    task_status.set_message("message","Succeed to purge fastly cache via url({})".format(purge_urls))

tasks_metadata = []


if settings.FASTLY_SERVICEID and settings.FASTLY_API_TOKEN and settings.FASTLY_SURROGATE_KEY:
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
