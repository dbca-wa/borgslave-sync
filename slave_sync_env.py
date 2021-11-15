import os
import re
import traceback
import json
import socket
import pytz
import sys
import logging
from jinja2 import Environment,FileSystemLoader
from geoserver.catalog import Catalog
from datetime import datetime

PATH = os.path.dirname(os.path.realpath(__file__))

DEFAULT_TIMEZONE = pytz.timezone('Australia/Perth')

CODE_PATH = PATH
STATE_PATH = os.environ.get("STATE_REPOSITORY_ROOT",os.path.split(CODE_PATH)[0])
VERSION_FILE = os.path.join(CODE_PATH,"version")

try:
    POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL",sys.argv[1] if len(sys.argv) > 1 else 60))
    if POLL_INTERVAL <= 0:
        POLL_INTERVAL = 60
except:
    POLL_INTERVAL = 60

CACHE_PATH = os.environ.get("CACHE_PATH",os.path.join(PATH, "dumps"))
if not os.path.exists(CACHE_PATH):   os.makedirs(CACHE_PATH)
os.chmod(CACHE_PATH,0755)

PUBLISH_PATH = os.path.join(PATH, "publish")
if not os.path.exists(PUBLISH_PATH):   os.makedirs(PUBLISH_PATH)
os.chmod(PUBLISH_PATH,0755)

PREVIEW_ROOT_PATH = os.path.join(PATH, "previews")
PREVIEW_ROOT_PATH = PREVIEW_ROOT_PATH[0:-1] if PREVIEW_ROOT_PATH[-1:] == "/" else PREVIEW_ROOT_PATH

if not os.path.exists(PREVIEW_ROOT_PATH):   os.makedirs(PREVIEW_ROOT_PATH)

SYNC_STATUS_PATH = os.environ.get("SYNC_STATUS_PATH",os.path.join(PATH,'.sync_status'))
if not os.path.exists(SYNC_STATUS_PATH):   os.makedirs(SYNC_STATUS_PATH)


DEBUG = bool(os.environ.get("DEBUG","false").lower() in ["true","yes","on"])
if DEBUG:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.WARNING)

ROLLBACK = DEBUG and bool(os.environ.get("ROLLBACK","false").lower() in ["true","yes","on"])

INCLUDE = [f for f in os.environ.get("INCLUDE","").split(",") if f.strip()]
HG_NODE = os.environ.get("HG_NODE", "0")
BORG_SSH = os.environ.get("BORG_SSH", "ssh -i /etc/id_rsa_borg -o StrictHostKeyChecking=no -o KeepAlive=yes -o ServerAliveInterval=30 -o ConnectTimeout=3600 -o ConnectionAttempts=5")
CODE_BRANCH = os.environ.get("CODE_BRANCH","default")
LISTEN_CHANNELS = set([c.strip() for c in os.environ.get("LISTEN_CHANNELS","kmi").split(",") if c.strip()])

url_re = re.compile("^(?P<protocol>https?)://(?P<host>[^:/\?]+)(:(?P<port>[0-9]+))?(?P<path>[^\?]+)?(\?(?P<params>.+)?)?$",re.IGNORECASE)
GEOSERVER_URL = [url.strip() for url in os.environ.get("GEOSERVER_URL", "http://localhost:8080/geoserver").split(",") if url and url.strip()]
GEOSERVER_URL = [(url[:-1] if url[-1] == "/" else url) for url in GEOSERVER_URL]
GEOSERVER_HOST = [url_re.search(url).group("host") for url in GEOSERVER_URL]
GEOSERVER_REST_URL =  [ os.path.join(url,"rest") for url in GEOSERVER_URL]

GEOSERVER_DATA_DIR = [d for d in os.environ.get("GEOSERVER_DATA_DIR", "/opt/geoserver_data").split(",") if d and d.strip()]
if len(GEOSERVER_DATA_DIR) == 1:
    GEOSERVER_DATA_DIR = GEOSERVER_DATA_DIR * len(GEOSERVER_URL)
elif len(GEOSERVER_DATA_DIR) != len(GEOSERVER_URL):
    raise Exception("Please configure the data dir for each geoserver")

GEOSERVER_THEME_DIR = [ os.path.join(d, "www/themes") for d in GEOSERVER_DATA_DIR]

GEOSERVER_USERNAME = [n.strip() for n in os.environ.get("GEOSERVER_USERNAME", "admin").split(",") if n and n.strip()]
if len(GEOSERVER_USERNAME) == 1 :
    GEOSERVER_USERNAME = GEOSERVER_USERNAME * len(GEOSERVER_URL)
elif len(GEOSERVER_USERNAME) != len(GEOSERVER_URL):
    raise Exception("Please configure the user name for each geoserver")

GEOSERVER_PASSWORD = [n.strip() for n in os.environ.get("GEOSERVER_PASSWORD", "geoserver").split(",") if n and n.strip()]
if len(GEOSERVER_PASSWORD) == 1 :
    GEOSERVER_PASSWORD = GEOSERVER_PASSWORD * len(GEOSERVER_URL)
elif len(GEOSERVER_PASSWORD) != len(GEOSERVER_URL):
    raise Exception("Please configure the password for each geoserver")

GEOSERVER_WMS_GETCAPABILITIES_URL = ["{}/wms?request=GetCapabilities&version=1.3.0&tiled=true".format(u) for u in GEOSERVER_URL]
gs = []
for index in range(len(GEOSERVER_URL)):
    gs.append(Catalog(GEOSERVER_REST_URL[index], GEOSERVER_USERNAME[index], GEOSERVER_PASSWORD[index]))

GEOSERVER_SHARING_DATA_DIR = os.environ.get("GEOSERVER_SHARING_DATA_DIR","false").lower() in ["true","yes"]

GEOSERVER_WORKSPACE_NAMESPACE = os.environ.get("GEOSERVER_WORKSPACE_NAMESPACE", "http://{}.dpaw.wa.gov.au")
GEOSERVER_DATASTORE_NAMESPACE = os.environ.get("GEOSERVER_DATASTORE_NAMESPACE", "{}_ds")
GEOSERVER_DEFAULT_CRS = os.environ.get("GEOSERVER_DEFAULT_CRS", "EPSG:4326")

GEOSERVER_PGSQL_HOST = os.environ.get("GEOSERVER_PGSQL_HOST", "localhost")
GEOSERVER_PGSQL_DATABASE = os.environ.get("GEOSERVER_PGSQL_DATABASE", "borg_slave")
GEOSERVER_PGSQL_SCHEMA = os.environ.get("GEOSERVER_PGSQL_SCHEMA", "publish")
GEOSERVER_PGSQL_PORT = os.environ.get("GEOSERVER_PGSQL_PORT", "5432")
GEOSERVER_PGSQL_USERNAME = os.environ.get("GEOSERVER_PGSQL_USERNAME", "test")
GEOSERVER_PGSQL_PASSWORD = os.environ.get("GEOSERVER_PGSQL_PASSWORD", "test")
SLAVE_NAME = os.environ.get("SLAVE_NAME",None) or socket.gethostname()
SYNC_SERVER = os.environ.get("SYNC_SERVER",None) or None
SYNC_PATH = os.environ.get("SYNC_PATH",None) or "/opt/dpaw-borg-state/code/dumps"

BORG_STATE_REPOSITORY = os.environ.get("BORG_STATE_REPOSITORY",os.path.join(os.path.split(PATH)[0],"dpaw-borg-state"))

SKIP_AUTH = os.environ.get("SKIP_AUTH", "false").lower() in ["true","yes"]
SKIP_RULES = os.environ.get("SKIP_RULES", "false").lower() in ["true","yes"]
SKIP_DB = os.environ.get("SKIP_DB", "false").lower() in ["true","yes"]
SKIP_GS = os.environ.get("SKIP_GS", "false").lower() in ["true","yes"]

SHARE_LAYER_DATA = os.environ.get("SHARE_LAYER_DATA","false").lower() in ["true","yes"]
SHARE_PREVIEW_DATA = os.environ.get("SHARE_PREVIEW_DATA","false").lower() in ["true","yes"]

FASTLY_ PURGE_URL = os.environ.get("FASTLY_PURGE_URL")
FASTLY_SERVICEID = os.environ.get("FASTLY_SERVICEID")
FASTLY_API_TOKEN = os.environ.get("FASTLY_API_TOKEN")

FASTLY_SOFT_PURGE = "1" if os.environ.get("FASTLY_SOFT_PURGE","false").lower() in ["true","yes"] else "0"
FASTLY_SURROGATE_KEY = [k.strip() for k in os.environ.get("FASTLY_SURROGATE_KEY","").split(",") if k and k.strip()]

FEATURE_FILTER = eval(os.environ.get("FEATURE_FILTER",None) or ("lambda job:job.get('auth_level',-1) in [0,1]" if SYNC_SERVER else "lambda job: True" ))
WMS_FILTER = eval(os.environ.get("WMS_FILTER",None) or ("lambda job: False" if SYNC_SERVER else "lambda job: True"))
LAYERGROUP_FILTER = eval(os.environ.get("LAYERGROUP_FILTER",None) or ("lambda job: False" if SYNC_SERVER else "lambda job: True"))

GEOSERVER_PGSQL_CONNECTION_DEFAULTS = {
    "host": GEOSERVER_PGSQL_HOST,
    "database": GEOSERVER_PGSQL_DATABASE,
    "schema": GEOSERVER_PGSQL_SCHEMA,
    "port": GEOSERVER_PGSQL_PORT,
    "user": GEOSERVER_PGSQL_USERNAME,
    "passwd": "plain:{}".format(GEOSERVER_PGSQL_PASSWORD),
    "Connection timeout": "20",
    "dbtype": "postgis",
    "Evictor tests per run": "3",
    "validate connections": "true",
    "encode functions": "false",
    "max connections": "10",
    "Support on the fly geometry simplification": "true",
    "Max connection idle time": "300",
    "Evictor run periodicity": "300",
    "Test while idle": "true",
    "Loose bbox": "true",
    "Expose primary keys": "true",
    "create database": "false",
    "Max open prepared statements": "50",
    "fetch size": "1000",
    "preparedStatements": "false",
    "Estimated extends": "true",
    "min connections": "1"
}
env = os.environ.copy()
env["PGPASSWORD"] = GEOSERVER_PGSQL_PASSWORD or "dummy"

template_env = Environment(loader=FileSystemLoader(CODE_PATH))

def get_version():
    version = None
    try:
        with open(VERSION_FILE) as f:
            version = f.read().strip()
    except:
        version = "0.0"

    return version

def now():
    """
    Return current time
    """
    return DEFAULT_TIMEZONE.localize(datetime.now())


remotefile_re = re.compile("^\s*(?P<user>[^\@]+)\@(?P<host>[^:]+):(?P<file>\S+)\s*$")
def parse_remotefilepath(f):
    m = remotefile_re.search(f)
    if not m:
        raise Exception("Failed to parse remote file path '{}'".format(f))
    return {
        "user":m.group("user"),
        "host":m.group("host"),
        "file":m.group("file")
    }


def apply_to_geoservers(sync_job,task_metadata,task_status,func,args_func=lambda index:(gs[index],),start=0,end=1 if GEOSERVER_SHARING_DATA_DIR else len(GEOSERVER_URL)):
    if len(GEOSERVER_URL[start:end]) == 1:
        func(sync_job,task_metadata,task_status,*args_func(0))
    else:
        exceptions = []
        for i in range(start,end):
            stagename = GEOSERVER_HOST[i]
            try:
                if task_status.is_stage_not_succeed(stagename):
                    task_status.del_stage_message(stagename,"message")
                    func(sync_job,task_metadata,task_status,*args_func(i),stage=stagename)
                    if not task_status.get_stage_message(stagename,"message"):
                        task_status.set_stage_message(stagename,"message","succeed")
                    task_status.stage_succeed(stagename)
            except:
                task_status.stage_failed(stagename)
                task_status.set_stage_message(stagename,"message",str(sys.exc_info()[1]))
                exceptions.append(str(sys.exc_info()[1]))
    
        if exceptions:
            raise Exception("\n".join(exceptions))
        elif task_status.all_stages_succeed:
            task_status.clean_task_failed()
        else:
            task_status.task_failed()

