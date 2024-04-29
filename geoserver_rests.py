import logging
logger = logging.getLogger(__name__)

def data_header(f = "xml"):
    if f == "xml":
        return {"content-type": "application/xml"}
    elif f == "json":
        return {"content-type": "application/json"}
    else:
        raise Exception("Format({}) Not Support".format(f))

def content_header(f = "xml")
    if f == "xml":
        return {"Accept": "application/xml"}
    elif f == "json":
        return {"Accept": "application/json"}
    else:
        raise Exception("Format({}) Not Support".format(f))

def workspaces_url(rest_url):
    return "{0}/workspaces".format(rest_url)

def workspace_url(rest_url,workspace):
    return "{0}/workspaces/{1}".format(rest_url,workspace)

def datastores_url(rest_url,workspace):
    return "{0}/workspaces/{1}/datastores".format(rest_url,workspace)

def datastore_url(rest_url,workspace,storename):
    return "{0}/workspaces/{1}/datastores/{2}".format(rest_url,workspace,storename)

postgis_connection_parameters = {
    "host": GEOSERVER_PGSQL_HOST,
    "database": GEOSERVER_PGSQL_DATABASE,
    "schema": GEOSERVER_PGSQL_SCHEMA,
    "port": GEOSERVER_PGSQL_PORT,
    "user": GEOSERVER_PGSQL_USERNAME,
    "passwd": "plain:{}".format(GEOSERVER_PGSQL_PASSWORD),
    "dbtype": "postgis",
    "Connection timeout": 20,
    "Evictor tests per run": 3,
    "validate connections": "true",
    "encode functions": "false",
    "max connections": 10,
    "Support on the fly geometry simplification": "true",
    "Max connection idle time": 300,
    "Evictor run periodicity": 300,
    "Test while idle": "true",
    "Loose bbox": "true",
    "Expose primary keys": "true",
    "create database": "false",
    "Max open prepared statements": 50,
    "fetch size": 1000,
    "preparedStatements": "false",
    "Estimated extends": "true",
    "min connections": 1,
    "Primary key metadata table":None,
    "Batch insert size":1,
    "Session startup SQL":None,
    "Session close-up SQL":None
}

env = os.environ.copy()

def featuretype_url(rest_url,workspace,storename,featurename):
    return "{0}/workspaces/{1}/datastores/{2}/featuretypes/{3}".format(rest_url,workspace,storename,featurename)

def styles_url(rest_url,workspace):
    return "{0}/workspaces/{1}/styles".format(rest_url,workspace)

def style_url(rest_url,workspace,stylename):
    return "{0}/workspaces/{1}/styles/{2}".format(rest_url,workspace,stylename)

def layer_url(rest_url,workspace,layername):
    return "{0}/layers/{1}:{2}".format(rest_url,workspace,layername)

def has_workspace(rest_url,workspace,username,password):
    res = requests.get(workspace_url(rest_url,workspace), auth=(username,password))
    if res.status_code == 200:
        return True
    else:
        return False

def create_workspace(rest_url,workspace,username,password):
    if has_workspace(rest_url,workspace,username,password):
        logger.info("The workspace({}) already exists".format(workspace))
        return

    data = """
<?xml version="1.0" encoding="UTF-8"?>
<workspace>
    <workspace>
        <name>{}</name>
    </workspace>
</workspace>
""".format(workspace)
    r = requests.post(workspace_url(rest_usr,workspace),data=data,headers=data_header("xml"),auth=(username,password))
    if r.status_code != 200:
        raise Exception("Failed to create datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))

def has_datastore(rest_url,workspace,storename,username,password):
    res = requests.get(datastore_url(rest_url,workspace,storename), auth=(username,password))
    if res.status_code == 200:
        return True
    else:
        return False

def create_datastore(rest_url,workspace,storename,username,password):
    if has_datastore(rest_url,workspace,storename,username,password):
        logger.info("Update datastore({}:{})".format(workspace,name))
        url = datastore_url(rest_url, workspace,storename)
        func = requests.put
    else:
        logger.info("Create datastore({}:{})".format(workspace,name))
        url = datastores_url(rest_url, workspace)
        func = requests.post

    connection_parameters = None
    for k,v in postgis_connection_parameters.items():
        if k in sync_job:
            value = str(sync_job[k])
        elif "geoserver_setting" in sync_job and k in sync_job["geoserver_setting"]:
            value = str(sync_job["geoserver_setting"][k])
        elif v:
            value = v
        else:
            continue

        if connection_parameters:
            connection_parameters = """{}{}<entry key="{}">{}</entry>""".format(connection_parameters,os.linesep,k,v)
        else:
            connection_parameters = """<entry key="{}">{}</entry>""".format(k,v)

    database_connection = """
<dataStore>
    <name>{}</name>
    <description>{}</description>
    <connectionParameters>
        {}
    <connectionParameters>
<dataStore>
""".format(storename,"local postgis datastore",connection_parameters)
    r = func(url,data=database_connection,headers=data_header(),auth=(username,password))
    if r.status_code == 200:
        return "Featurestore created/updated successfully"
    else:
        raise Exception("Failed to create datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))

def delete_datastore(rest_url,workspace,storename,username,password):
    if has_datastore(workspace,storename):
        r = requests.delete("{}?recurse=false".format(datastore_url(rest_url,workspace,storename)),auth=(username,password))
        if r.status_code != 200:
            raise Exception("Failed to delete datastore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))
    else:
        logger.info("The datastore({}:{}) doesn't exist".format(workspace,storename))

def has_featuretype(rest_url,workspace,storename,layername,username,password):
    r = requests.get(featuretype_url(rest_url,workspace,storename,layername),headers=content_header(),auth=(username,password))
    if r.status_code == 200:
        return True
    else:
        return False

def cr

def delete_featuretype(rest_url,workspace,storename,layername,username,password):
    if not has_featuretype(rest_url,workspace,storename,layername,username,password):
        logger.info("The featuretype({}:{}) doesn't exist".format(workspace,layername))
        return

        gs.delete(l_gs)

    r = requests.delete("{}?recurse=false".format(featuretype_url(rest_url,workspace,storename,layername)),auth=(username,password))
    if r != 200:
        raise Exception("Failed to delete the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

def has_style(rest_url,workspace,stylename,username,password):
    r = requests.get(style_url(rest_url,workspace,stylename), auth=(username,password))
    if r.status_code == 200:
        return True
    else:
        return False

def delete_style(rest_url,workspace,stylename,username,password):
    r = requests.delete("{}?recurse=false&purge=true".format(style_url(rest_url,stylename)), auth=(username,password))
    if r != 200:
        raise Exception("Failed to delete the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))

def update_style(rest_url,workspace,stylename,sldversion,slddata,username,password):
    sld_content_type = "application/vnd.ogc.sld+xml"
    if sld_version == "1.1.0" or sld_version == "1.1":
        sld_content_type = "application/vnd.ogc.se+xml"

    headers = {"content-type": sld_content_type}

    style_xml = "<style><name>{0}</name><filename>{0}.sld</filename></style>".format(stylename))

    if has_style(rest_url,workspace,stylename,username,password):
        r = requests.put(style_url(rest_url,workspace,stylename),data=style_xml, headers=headers,auth=(username,password))
    else:
        r = requests.post(styles_url(rest_url,workspace),data=style_xml, headers=headers,auth=(username,password))
    if r_sld.status_code != 200:
        raise Exception("Failed to update the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))

def get_layer_styles(rest_url,workspace,layername,username,password):
    """
    Return a tuple(default style, alternate styles)
    """
    r = requests.get(layer_url(rest_url,workspace,layername),headers=content_header("json"),auth=(username,password))
    if r.status_code == 200:
        r = r.json()
        return (r.get("defaultStyle",{}).get("name",None), [d["name"] for d in r.get("styles",{}).get("style",[]])
    else:
        raise Exception("Failed to get styles of featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

