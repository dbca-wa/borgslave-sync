import os
import traceback
import json
import socket
import pytz
from jinja2 import Environment,FileSystemLoader
from geoserver.catalog import Catalog
from datetime import datetime

PATH = os.path.dirname(os.path.realpath(__file__))

DEFAULT_TIMEZONE = pytz.timezone('Australia/Perth')

CODE_PATH = PATH
STATE_PATH = os.path.split(CODE_PATH)[0]
VERSION_FILE = os.path.join(CODE_PATH,"version")

CACHE_PATH = os.path.join(PATH, "dumps")
if not os.path.exists(CACHE_PATH):   os.makedirs(CACHE_PATH)
os.chmod(CACHE_PATH,0755)

PUBLISH_PATH = os.path.join(PATH, "publish")
if not os.path.exists(PUBLISH_PATH):   os.makedirs(PUBLISH_PATH)
os.chmod(PUBLISH_PATH,0755)

PREVIEW_ROOT_PATH = os.path.join(PATH, "previews")
if not os.path.exists(PREVIEW_ROOT_PATH):   os.makedirs(PREVIEW_ROOT_PATH)

SYNC_STATUS_PATH = os.path.join(PATH,'.sync_status') 
if not os.path.exists(SYNC_STATUS_PATH):   os.makedirs(SYNC_STATUS_PATH)


DEBUG = bool(os.environ.get("DEBUG","false").lower() in ["true","yes","on"])
INCLUDE = [f for f in os.environ.get("INCLUDE","").split(",") if f.strip()]
HG_NODE = os.environ.get("HG_NODE", "0")
BORG_SSH = os.environ.get("BORG_SSH", "ssh -i /etc/id_rsa_borg -o StrictHostKeyChecking=no -o KeepAlive=yes -o ServerAliveInterval=30 -o ConnectTimeout=3600 -o ConnectionAttempts=5")
CODE_BRANCH = os.environ.get("CODE_BRANCH","default")
LISTEN_CHANNELS = set([c.strip() for c in os.environ.get("LISTEN_CHANNELS","kmi").split(",") if c.strip()])
GEOSERVER_URL = os.environ.get("GEOSERVER_URL", "http://localhost:8080/geoserver")
GEOSERVER_REST_URL = "/".join([GEOSERVER_URL,"rest/"])
GEOSERVER_DATA_DIR = os.environ.get("GEOSERVER_DATA_DIR", "/opt/geoserver/data_dir")
GEOSERVER_USERNAME = os.environ.get("GEOSERVER_USERNAME", "admin")
GEOSERVER_PASSWORD = os.environ.get("GEOSERVER_PASSWORD", "geoserver")
GEOSERVER_WORKSPACE_NAMESPACE = os.environ.get("GEOSERVER_WORKSPACE_NAMESPACE", "http://{}.dpaw.wa.gov.au")
GEOSERVER_DATASTORE_NAMESPACE = os.environ.get("GEOSERVER_DATASTORE_NAMESPACE", "{}_ds")
GEOSERVER_WMSLAYERS_REST_URL = os.environ.get("GEOSERVER_WMSLAYERS_REST_URL",GEOSERVER_REST_URL + 'workspaces/{}/wmsstores/{}/wmslayers.xml')
GEOSERVER_WMSLAYER_REST_URL = os.environ.get("GEOSERVER_WMSLAYER_REST_URL",GEOSERVER_REST_URL + 'workspaces/{}/wmsstores/{}/wmslayers/{}.xml?recurse=true')
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
    "Expose primary keys": "false",
    "create database": "false",
    "Max open prepared statements": "50",
    "fetch size": "1000",
    "preparedStatements": "false",
    "Estimated extends": "true",
    "min connections": "1"
}
GEOSERVER_DEFAULT_CRS = os.environ.get("GEOSERVER_DEFAULT_CRS", "EPSG:4326")

env = os.environ.copy()
env["PGPASSWORD"] = GEOSERVER_PGSQL_PASSWORD or "dummy"

gs = Catalog(GEOSERVER_REST_URL, GEOSERVER_USERNAME, GEOSERVER_PASSWORD)

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
