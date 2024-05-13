import logging
import collections
import slave_sync_env as settings
import os
import string
import requests

logger = logging.getLogger(__name__)

def encode_xmltext(text):
    if not text:
        return ""
    result = None
    for i in range(len(text)):
        if text[i] not in ['&'] and text[i] in string.printable:
            if result :
                result += text[i]
        else:
            if result is None:
                result = text[:i]
            result += "&#{};".format(ord(text[i]))

    return result if result else text
def contenttype_header(f = "xml"):
    if f == "xml":
        return {"content-type": "application/xml"}
    elif f == "json":
        return {"content-type": "application/json"}
    else:
        raise Exception("Format({}) Not Support".format(f))

def accept_header(f = "xml"):
    if f == "xml":
        return {"Accept": "application/xml"}
    elif f == "json":
        return {"Accept": "application/json"}
    elif f == "html":
        return {"Accept": "text/html"}
    else:
        raise Exception("Format({}) Not Support".format(f))

generic_style   = """<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
                       xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
                       xmlns="http://www.opengis.net/sld" 
                       xmlns:ogc="http://www.opengis.net/ogc" 
                       xmlns:xlink="http://www.w3.org/1999/xlink" 
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>{0}</Name>
    <UserStyle>
      <Name>{0}</Name>
      <Title>A orange generic style</Title>
      <FeatureTypeStyle>
        <Rule>
          <Name>raster</Name>
          <Title>raster</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="isCoverage"/>
              <ogc:Literal>true</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
          </RasterSymbolizer>
        </Rule>
        <Rule>
          <Title>orange polygon</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="dimension">
                <ogc:Function name="geometry"/>
              </ogc:Function>
              <ogc:Literal>2</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill>
              <CssParameter name="fill">#ff6600</CssParameter>
            </Fill>
            <Stroke>
              <CssParameter name="stroke">#000000</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>orange line</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="dimension">
                <ogc:Function name="geometry"/>
              </ogc:Function>
              <ogc:Literal>1</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <LineSymbolizer>
            <Stroke>
              <CssParameter name="stroke">#ff6600</CssParameter>
              <CssParameter name="stroke-opacity">1</CssParameter>
            </Stroke>
          </LineSymbolizer>
        </Rule>
        <Rule>
          <Title>orange point</Title>
          <ElseFilter/>
          <PointSymbolizer>
            <Graphic>
              <Mark>
                <WellKnownName>square</WellKnownName>
                <Fill>
                  <CssParameter name="fill">#ff6600</CssParameter>
                </Fill>
              </Mark>
              <Size>6</Size>
            </Graphic>
          </PointSymbolizer>
        </Rule>
        <VendorOption name="ruleEvaluation">first</VendorOption>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
"""

def reload_url(geoserver_url):
    return "{0}/rest/reload".format(geoserver_url)

def workspaces_url(geoserver_url):
    return "{0}/rest/workspaces".format(geoserver_url)

def workspace_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}".format(geoserver_url,workspace)

def datastores_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}/datastores".format(geoserver_url,workspace)

def datastore_url(geoserver_url,workspace,storename):
    return "{0}/rest/workspaces/{1}/datastores/{2}".format(geoserver_url,workspace,storename)

postgis_connection_parameters = {
    "host": settings.GEOSERVER_PGSQL_HOST,
    "database": settings.GEOSERVER_PGSQL_DATABASE,
    "schema": settings.GEOSERVER_PGSQL_SCHEMA,
    "port": settings.GEOSERVER_PGSQL_PORT,
    "user": settings.GEOSERVER_PGSQL_USERNAME,
    "passwd": "plain:{}".format(settings.GEOSERVER_PGSQL_PASSWORD),
    "SSL mode":"ALLOW",
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

def featuretypes_url(geoserver_url,workspace,storename):
    return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes".format(geoserver_url,workspace,storename)

def featuretype_url(geoserver_url,workspace,storename,featurename):
    if storename:
        return "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}".format(geoserver_url,workspace,storename,featurename)
    else:
        return "{0}/rest/workspaces/{1}/featuretypes/{2}".format(geoserver_url,workspace,featurename)

def styles_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}/styles".format(geoserver_url,workspace)

def style_url(geoserver_url,workspace,stylename):
    return "{0}/rest/workspaces/{1}/styles/{2}".format(geoserver_url,workspace,stylename)

def layer_styles_url(geoserver_url,workspace,layername):
    return "{0}/rest/layers/{1}:{2}".format(geoserver_url,workspace,layername)

def catalogue_mode_url(geoserver_url):
    return "{0}/rest/security/acl/catalog".format(geoserver_url)

def layer_access_rules_url(geoserver_url):
    return "{0}/rest/security/acl/layers".format(geoserver_url)

def layer_access_rule_url(geoserver_url,layerrule):
    return "{0}/rest/security/acl/layers/{1}".format(geoserver_url,layerrule)

def wmsstores_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}/wmsstores".format(geoserver_url,workspace)

def wmsstore_url(geoserver_url,workspace,storename):
    return "{0}/rest/workspaces/{1}/wmsstores/{2}".format(geoserver_url,workspace,storename)

def wmsstore_layers_url(geoserver_url,workspace,storename,f=None):
    return "{0}/rest/workspaces/{1}/wmsstores/{2}/wmslayers".format(geoserver_url,workspace,storename)

def wmsstore_layer_url(geoserver_url,workspace,storename,layername,f=None):
    return "{0}/rest/workspaces/{1}/wmsstores/{2}/wmslayers/{3}{4}".format(geoserver_url,workspace,storename,layername,".{}".format(f) if f else "")

def wmslayers_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}/wmslayers".format(geoserver_url,workspace,storename)

def wmslayer_url(geoserver_url,workspace,layername,f=None):
    return "{0}/rest/workspaces/{1}/wmslayers/{2}{3}".format(geoserver_url,workspace,layername,".{}".format(f) if f else "")

def layergroups_url(geoserver_url,workspace):
    return "{0}/rest/workspaces/{1}/layergroups".format(geoserver_url,workspace)

def layergroup_url(geoserver_url,workspace,groupname):
    return "{0}/rest/workspaces/{1}/layergroups/{2}".format(geoserver_url,workspace,groupname)

def gwc_layers_url(geoserver_url):
    return "{0}/gwc/rest/layers".format(geoserver_url)

def gwc_layer_url(geoserver_url,workspace,layername,f=None):
    return "{0}/gwc/rest/layers/{1}:{2}{3}".format(geoserver_url,workspace,layername,".{}".format(f) if f else "")

def gwc_layer_seed_url(geoserver_url,workspace,layername):
    return "{0}/gwc/rest/seed/{1}:{2}.xml".format(geoserver_url,workspace,layername)

def has_workspace(geoserver_url,username,password,workspace):
    res = requests.get(workspace_url(geoserver_url,workspace), auth=(username,password))
    return True if res.status_code == 200 else False

def list_workspaces(geoserver_url,username,password):
    """
    Return unordered workspace list.
    """
    r = requests.get(workspaces_url(geoserver_url),headers=accept_header("json"), auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the workspaces. code = {},message = {}".format(r.status_code, r.content))

    return [w["name"] for w in (r.json().get("workspaces") or {}).get("workspace") or [] ]

def create_workspace(geoserver_url,username,password,workspace):
    data = """<?xml version="1.0" encoding="UTF-8"?>
<workspace>
    <name>{}</name>
</workspace>
""".format(workspace)
    r = requests.post(workspaces_url(geoserver_url),data=data,headers=contenttype_header("xml"),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to create the workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))

    logger.debug("Succeed to create the workspace({})".format(workspace))

def has_datastore(geoserver_url,username,password,workspace,storename):
    res = requests.get(datastore_url(geoserver_url,workspace,storename), auth=(username,password))
    return True if res.status_code == 200 else False

def list_datastores(geoserver_url,username,password,workspace):
    r = requests.get(datastores_url(geoserver_url,workspace), headers=accept_header("json"), auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the datastores in workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))

    return [d["name"] for d in (r.json().get("dataStores") or {}).get("dataStore") or [] ]

def update_datastore(geoserver_url,username,password,workspace,storename,parameters,create=None):
    if create is None:
        #check whether datastore exists or not.
        create = False if has_datastore(geoserver_url,username,password,workspace,storename) else True

    if create:
        url = datastores_url(geoserver_url, workspace)
        func = requests.post
    else:
        url = datastore_url(geoserver_url, workspace,storename)
        func = requests.put

    connection_parameters = None
    for k,v in postgis_connection_parameters.items():
        if k in parameters:
            if parameters[k] is None:
                continue
            else:
                value = str(parameters[k])
        elif v:
            value = v
        elif v is None:
            continue

        if connection_parameters:
            connection_parameters = """{}{}<entry key="{}">{}</entry>""".format(connection_parameters,os.linesep,k,value)
        else:
            connection_parameters = """<entry key="{}">{}</entry>""".format(k,value)

    store_data = """<?xml version="1.0" encoding="UTF-8"?>
<dataStore>
    <name>{}</name>
    <description>{}</description>
    <connectionParameters>
        {}
    </connectionParameters>
</dataStore>
""".format(storename,"local postgis datastore",connection_parameters)
    r = func(url,data=store_data,headers=contenttype_header(),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to {} the datastore({}:{}). code = {},message = {}".format("create" if create else "update",workspace,storename,r.status_code, r.content))

    logger.debug("Succeed to {} the datastore({}:{})".format("create" if create else "update",workspace,storename))

def delete_datastore(geoserver_url,username,password,workspace,storename):
    if not has_datastore(geoserver_url,username,password,workspace,storename):
        logger.debug("The datastore({}:{}) doesn't exists".format(workspace,storename))
        return

    r = requests.delete("{}?recurse=false".format(datastore_url(geoserver_url,workspace,storename)),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to delete datastore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))

    logger.debug("Succeed to delete the datastore({}:{})".format(workspace,storename))

def has_featuretype(geoserver_url,username,password,workspace,storename,layername):
    r = requests.get(featuretype_url(geoserver_url,workspace,storename,layername),headers=accept_header(),auth=(username,password))
    return True if r.status_code == 200 else False

def list_featuretypes(geoserver_url,username,password,workspace,storename):
    r = requests.get(featuretypes_url(geoserver_url,workspace,storename),headers=accept_header("json"),  auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the featuretypes in datastore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))

    return [f["name"] for f in (r.json().get("featureTypes") or {}).get("featureType") or [] ]

def publish_featuretype(geoserver_url,username,password,workspace,storename,layername,parameters):
    if parameters.get('viewsql'):
        featuretype_data = """<?xml version="1.0" encoding="UTF-8"?>
<featureType>
    <name>{2}</name>
    <namespace>
        <name>{0}</name>
    </namespace>
    {3}
    {4}
    <keywords>
        {5}
    </keywords>
    <srs>{6}</srs>
    {7}
    {8}
    <enabled>true</enabled>
    <store class="dataStore">
        <name>{0}:{1}</name>
    </store>
    <metadata>
        <entry key="JDBC_VIRTUAL_TABLE">
            <virtualTable>
                <name>{2}</name>
                <sql><![CDATA[{9}]]></sql>
                <escapeSql>{10}</escapeSql>
                <geometry>
                    <name>{11}</name>
                    <type>{12}</type>
                    <srid>{13}</srid>
                </geometry>
            </virtualTable>
        </entry>
  </metadata>
</featureType>
""".format(
    workspace,
    storename,
    layername,
    "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "", 
    "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
    os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
    parameters.get("srs","EPSG:4326"),
    """
    <nativeBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </nativeBoundingBox>
""".format(*parameters["nativeBoundingBox"]) if parameters.get("nativeBoundingBox") else "",
    """
    <latLonBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </latLonBoundingBox>
""".format(*parameters["latLonBoundingBox"]) if parameters.get("latLonBoundingBox") else "",
    parameters.get('viewsql'),
    parameters.get("escapeSql","false"),
    parameters.get("spatial_column"),
    parameters.get("spatial_type"),
    parameters.get("srs","EPSG:4326")[5:]
)
    else:
        featuretype_data = """<?xml version="1.0" encoding="UTF-8"?>
<featureType>
    <name>{2}</name>
    {9}
    <namespace>
        <name>{0}</name>
    </namespace>
    {3}
    {4}
    <keywords>
        {5}
    </keywords>
    <srs>{6}</srs>
    {7}
    {8}
    <enabled>true</enabled>
    <store class="dataStore">
        <name>{0}:{1}</name>
    </store>
</featureType>
""".format(
    workspace,
    storename,
    layername,
    "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "",
    "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
    os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
    parameters.get("srs","EPSG:4326"),
    """
    <nativeBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </nativeBoundingBox>
""".format(*parameters["nativeBoundingBox"]) if parameters.get("nativeBoundingBox") else "",
    """
    <latLonBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </latLonBoundingBox>
""".format(*parameters["latLonBoundingBox"]) if parameters.get("latLonBoundingBox") else "",
    "<nativeName>{}</nativeName>".format(parameters.get('table')) if parameters.get('table') else ""
)

    r = requests.post(featuretypes_url(geoserver_url,workspace,storename),headers=contenttype_header("xml"),data=featuretype_data,auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to create the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    logger.debug("Succeed to publish the featuretype({}:{})".format(workspace,layername))

def delete_featuretype(geoserver_url,username,password,workspace,storename,layername):
    if not has_featuretype(geoserver_url,username,password,workspace,storename,layername):
        if gwc_has_layer(geoserver_url,username,password,workspace,layername):
            gwc_delete_layer(geoserver_url,username,password,workspace,layername)
        return

    r = requests.delete("{}?recurse=true".format(featuretype_url(geoserver_url,workspace,storename,layername)),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to delete the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    logger.debug("Succeed to delete the featuretype({}:{})".format(workspace,layername))

def has_style(geoserver_url,username,password,workspace,stylename):
    r = requests.get(style_url(geoserver_url,workspace,stylename),headers=accept_header(), auth=(username,password))
    return True if r.status_code == 200 else False

def delete_style(geoserver_url,username,password,workspace,stylename):
    if not has_style(geoserver_url,username,password,workspace,stylename):
        return

    r = requests.delete("{}?recurse=true&purge=true".format(style_url(geoserver_url,workspace,stylename)), auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to delete the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))

    logger.debug("Succeed to delete the style({}:{})".format(workspace,stylename))

def update_style(geoserver_url,username,password,workspace,stylename,sldversion,slddata):
    if not has_style(geoserver_url,username,password,workspace,stylename):
        headers = {"content-type": "application/vnd.ogc.sld+xml"}
        placeholder_data = generic_style.format(stylename)
        r = requests.post(styles_url(geoserver_url,workspace),data=placeholder_data, headers=headers,auth=(username,password))
        if r.status_code >= 300:
            logger.error("Failed to create the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
            raise Exception("Failed to create the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))


    sld_content_type = "application/vnd.ogc.sld+xml"
    if sldversion == "1.1.0" or sldversion == "1.1":
        sld_content_type = "application/vnd.ogc.se+xml"

    headers = {"content-type": sld_content_type}

    r = requests.put(style_url(geoserver_url,workspace,stylename),data=slddata, headers=headers,auth=(username,password))

    if r.status_code >= 300:
        logger.error("Failed to update the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
        raise Exception("Failed to update the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))

    logger.debug("Succeed to update the style({}:{})".format(workspace,stylename))

def get_layer_styles(geoserver_url,username,password,workspace,layername):
    """
    Return a tuple(default style, alternate styles)
    """
    r = requests.get(layer_styles_url(geoserver_url,workspace,layername),headers=accept_header("json"),auth=(username,password))
    if r.status_code == 200:
        r = r.json()
        return (r.get("defaultStyle",{}).get("name",None), [d["name"] for d in r.get("styles",{}).get("style",[])])
    else:
        raise Exception("Failed to get styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

def set_layer_styles(geoserver_url,username,password,workspace,layername,default_style,styles):
    layer_styles_data = """<?xml version="1.0" encoding="UTF-8"?>
<layer>
  {0}
  <styles class="linked-hash-set">
  {1}
  </styles>
</layer>
""".format("<defaultStyle><name>{}</name></defaultStyle>".format(default_style) if default_style else "",os.linesep.join("<style><name>{}</name></style>".format(n) for n in styles) if styles else "")
    r = requests.put(layer_styles_url(geoserver_url,workspace,layername),headers=contenttype_header("xml"),data=layer_styles_data,auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to set styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    logger.debug("Succeed to set the styles of the layer({}:{}),default_style={}, styles={}".format(workspace,layername,default_style,styles))

def get_catalogue_mode(geoserver_url,username,password):
    r = requests.get(catalogue_mode_url(geoserver_url),headers=accept_header("json"),auth=(username,password))
    if r.status_code == 200:
        r = r.json()
        return r["mode"]
    else:
        raise Exception("Failed to get catalogue mode. code = {} , message = {}".format(r.status_code, r.content))

def set_catalogue_mode(geoserver_url,username,password,mode):
    data="""<?xml version="1.0" encoding="UTF-8"?><catalog>
<mode>{}</mode>
</catalog>""".format(mode)
    r = requests.put(catalogue_mode_url(geoserver_url),data = data,headers=contenttype_header("xml"),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to set the catalogue mode({}). code = {} , message = {}".format(mode,r.status_code, r.content))

    logger.debug("Succeed to set catalogue mode.")

def get_layer_access_rules(geoserver_url,username,password):
    r = requests.get(layer_access_rules_url(geoserver_url),headers=accept_header("json"),auth=(username,password))
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception("Failed to get catalogue mode. code = {} , message = {}".format(r.status_code, r.content))

def update_layer_access_rules(geoserver_url,username,password,layer_access_rules):
    existing_layer_access_rules = get_layer_access_rules(geoserver_url,username,password)
    new_layer_access_rules = {}
    update_layer_access_rules = {}

    #delete the not required layer access rules
    for permission,groups in existing_layer_access_rules.items():
        if permission not in layer_access_rules:
            r = requests.delete(layer_access_rule_url(geoserver_url,permission),auth=(username,password))
            if r.status_code >= 300:
                raise Exception("Failed to delete layer access rules({} = {}). code = {} , message = {}".format(permission,groups,r.status_code, r.content))
        else:
            update_layer_access_rules[permission] = layer_access_rules[permission]

    #get new layer access rules
    for permission,groups in layer_access_rules.items():
        if permission not in update_layer_access_rules:
            new_layer_access_rules[permission] = groups

    if update_layer_access_rules:
        data="""<?xml version="1.0" encoding="UTF-8"?><rules>
{}
</rules>""".format(os.linesep.join("""<rule resource="{0}">{1}</rule>""".format(k,v) for k,v in update_layer_access_rules.items()))
        r = requests.put(layer_access_rules_url(geoserver_url),data=data,headers=contenttype_header("xml"),auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to update layer access rules({}). code = {} , message = {}".format(update_layer_access_rules,r.status_code, r.content))

    if new_layer_access_rules:
        data="""<?xml version="1.0" encoding="UTF-8"?><rules>
{}
</rules>""".format(os.linesep.join("""<rule resource="{0}">{1}</rule>""".format(k,v) for k,v in new_layer_access_rules.items()))
        r = requests.post(layer_access_rules_url(geoserver_url),data=data,headers=contenttype_header("xml"),auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to create layer access rules({}). code = {} , message = {}".format(new_layer_access_rules,r.status_code, r.content))

    logger.debug("Succeed to update the layer access rules.")

def reload(geoserver_url,username,password):
    r = requests.put(reload_url(geoserver_url),auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to reload geoserver catalogue. code = {} , message = {}".format(r.status_code, r.content))
    else:
        logger.debug("Succeed to reload the geoserver catalogue.")

def gwc_has_layer(geoserver_url,username,password,workspace,layername):
    r = requests.get(gwc_layer_url(geoserver_url,workspace,layername,f="json"),headers=accept_header("json"), auth=(username,password))
    return True if r.status_code == 200 else False

def gwc_delete_layer(geoserver_url,username,password,workspace,layername):
    if gwc_has_layer(geoserver_url,username,password,workspace,layername):
        r = requests.delete(gwc_layer_url(geoserver_url,workspace,layername,f="xml"), auth=(username,password))
        if r.status_code >= 300:
            raise Exception("Failed to delete the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
        logger.debug("Succeed to delete the gwc layer({}:{})".format(workspace,layername))
    else:
        logger.debug("The gwc layer({}:{}) doesn't exist".format(workspace,layername))

def gwc_update_layer(geoserver_url,username,password,workspace,layername,parameters):
    layer_data = """<?xml version="1.0" encoding="UTF-8"?>
<GeoServerLayer>
    <name>{0}:{1}</name>
    <mimeFormats>
        <string>image/png</string>
        <string>image/jpeg</string>
        {2}
    </mimeFormats>
    <enabled>true</enabled>
    <inMemoryCached>true</inMemoryCached>
    <gridSubsets>
        <gridSubset>
            <gridSetName>gda94</gridSetName>
        </gridSubset>
        <gridSubset>
            <gridSetName>mercator</gridSetName>
        </gridSubset>
    </gridSubsets>
    <metaWidthHeight>
        <int>1</int>
        <int>1</int>
    </metaWidthHeight>
    <expireCache>{3}</expireCache>
    <expireClients>{4}</expireClients>
    <parameterFilters>
        <styleParameterFilter>
            <key>STYLES</key>
            <defaultValue></defaultValue>
        </styleParameterFilter>
    </parameterFilters>
    <gutter>100</gutter>
</GeoServerLayer>
""".format(
    workspace,
    layername,
    """
        <string>application/json;type=geojson</string>
        <string>application/json;type=topojson</string>
        <string>application/x-protobuf;type=mapbox-vector</string>
        <string>application/json;type=utfgrid</string>
""" if parameters.get("service_type") == "WFS" else "",
    parameters.get("geoserver_setting",{}).get("server_cache_expire"),
    parameters.get("geoserver_setting",{}).get("client_cache_expire")
)

    r = requests.put(gwc_layer_url(geoserver_url,workspace,layername,f="xml"), auth=(username,password), headers=contenttype_header("xml"), data=layer_data)
        
    if r.status_code >= 300:
        raise Exception("Failed to update the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    logger.debug("Succeed to update the gwc layer({}:{}). ".format(workspace,layername))

def gwc_empty_layer(geoserver_url,username,password,workspace,layername):
    for gridset in ("gda94","mercator"):
        for f in ("image/png","image/jpeg"):
            layer_data = """<?xml version="1.0" encoding="UTF-8"?>
<seedRequest>
    <name>{0}:{1}</name>
    <gridSetId>{2}</gridSetId>
    <zoomStart>0</zoomStart>
    <zoomStop>24</zoomStop>
    <type>truncate</type>
    <format>{3}</format>
    <threadCount>1</threadCount>
</seedRequest>
""".format(
    workspace,
    layername,
    gridset,
    f
)
            r = requests.post(gwc_layer_seed_url(geoserver_url,workspace,layername),auth=(username,password),headers=collections.ChainMap(accept_header("json"),contenttype_header("xml")), data=layer_data)
            if r.status_code >= 400:
                raise Exception("Failed to empty the cache of the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    #check whether the task is finished or not.
    finished = False
    while(finished):
        finished = True
        r = requests.get(gwc_layer_url(geoserver_url,workspace,layername), auth=(username,password), headers=accept_header("json"))
        if r.status_code >= 400:
            raise Exception("Failed to empty the cache of the gwc layer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

        tasks=r.json().get("long-array-array",[])
        for t in tasks:
            if t[3] == -1:
                #aborted
                raise Exception("Failed to empty the cache of the gwc layer({}:{}). some tasks are aborted".format(workspace,layername))
            elif t[3] in (0,1):
                finished = False
                break
        if not finished:
            time.sleep(1)


def has_wmsstore(geoserver_url,username,password,workspace,storename):
    r = requests.get(wmsstore_url(geoserver_url,workspace,storename),headers=accept_header("json"), auth=(username,password))
    return True if r.status_code == 200 else False

def list_wmsstores(geoserver_url,username,password,workspace):
    r = requests.get(wmsstores_url(geoserver_url,workspace),headers=accept_header("json"),  auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the wmsstores in workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))

    return [s["name"] for s in (r.json().get("wmsStores") or {}).get("wmsStore") or [] ]

def update_wmsstore(geoserver_url,username,password,workspace,storename,parameters):
    """
    update a store
    """
    if has_wmsstore(geoserver_url,username,password,workspace,storename):
        func = requests.put
        url = wmsstore_url(geoserver_url,workspace,storename)
    else:
        func = requests.post
        url = wmsstores_url(geoserver_url,workspace)

    store_data="""<?xml version="1.0" encoding="UTF-8"?>
<wmsStore>
    <name>{1}</name>
    <type>WMS</type>
    <enabled>true</enabled>
    <workspace>
        <name>{0}</name>
    </workspace>
    <metadata>
        <entry key="useConnectionPooling">true</entry>
    </metadata>
    <__default>true</__default>
    <capabilitiesURL><![CDATA[{2}]]></capabilitiesURL>
    {3}
    {4}
    {5}
    {6}
    {7}
</wmsStore>
""".format(
    workspace,
    storename,
    parameters.get("capability_url"),
    "<user>{}</user>".format(parameters.get("username") or ""),
    "<password>{}</password>".format(parameters.get("password") or ""),
    "<maxConnections>{}</maxConnections>".format(parameters.get("max_connections")) if parameters.get("max_connections") else "",
    "<readTimeout>{}</readTimeout>".format(parameters.get("read_timeout")) if parameters.get("read_timeout") else "",
    "<connectTimeout>{}</connectTimeout>".format(parameters.get("connect_timeout")) if parameters.get("connect_timeout") else ""
)

    r = func(url, auth=(username, password), headers=contenttype_header("xml"), data=store_data)
    if r.status_code >= 300:
        raise Exception("Failed to create the wmsstore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))

    logger.debug("Succeed to create the wmsstore({}:{}). ".format(workspace,storename))

def delete_wmsstore(geoserver_url,username,password,workspace,storename):
    if not has_wmsstore(geoserver_url,username,password,workspace,storename):
        logger.debug("The wmsstore({}:{}) doesn't exist".format(workspace,storename))
        return

    r = requests.delete("{}?recurse=false".format(wmsstore_url(geoserver_url,workspace,storename)), auth=(username, password))
    if r.status_code >= 300:
        raise Exception("Failed to delete wmsstore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))

    logger.debug("Succeed to delete the wmsstore({}:{})".format(workspace,storename))

def has_wmslayer(geoserver_url,username,password,workspace,layername,storename=None):
    url = wmsstore_layer_url(geoserver_url,workspace,storename,layername) if storename else wmslayer_url(geoserver_url,workspace,layername,f="json")
    r = requests.get(url,headers=accept_header("json"), auth=(username,password))
    return True if r.status_code == 200 else False

def list_wmslayers(geoserver_url,username,password,workspace,storename):
    r = requests.get(wmsstore_layers_url(geoserver_url,workspace,storename),headers=accept_header("json"),  auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the wmslayers in wmsstore({}:{}). code = {},message = {}".format(workspace,storename,r.status_code, r.content))

    return [l["name"] for l in (r.json().get("wmsLayers") or {}).get("wmsLayer") or [] ]

def delete_wmslayer(geoserver_url,username,password,workspace,layername):
    if not has_wmslayer(geoserver_url,username,password,workspace,layername):
        logger.debug("The wmslayer({}:{}) doesn't exist".format(workspace,layername))
        if gwc_has_layer(geoserver_url,username,password,workspace,layername):
            gwc_delete_layer(geoserver_url,username,password,workspace,layername)
        return

    r = requests.delete("{}?recurse=true".format(wmslayer_url(geoserver_url,workspace,layername,f="xml")), auth=(username, password))
    if r.status_code >= 300:
        raise Exception("Failed to delete the wmslayer({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))

    logger.debug("Succeed to delete the wmslayer({}:{}).".format(workspace,layername))


def update_wmslayer(geoserver_url,username,password,workspace,storename,layername,parameters):
    if has_wmslayer(geoserver_url,username,password,workspace,layername):
        if has_wmslayer(geoserver_url,username,password,workspace,layername,storename=storename):
            #layer exists and in the same wmsstore
            func = requests.put
            url = wmslayer_url(geoserver_url,workspace,layername,f="xml")
        else:
            #layer exists,but in different wmsstore
            #delete the wmslayer and recreate it
            delete_wmslayer(geoserver_url,username,password,workspace,layername)
            func = requests.post
            url = wmsstore_layers_url(geoserver_url,workspace,storename)
    else:
        #layer doesn't exist
        func = requests.post
        url = wmsstore_layers_url(geoserver_url,workspace,storename)

    layer_data="""<?xml version="1.0" encoding="UTF-8"?>
<wmsLayer>
    <name>{2}</name>
    {6}
    <namespace>
        <name>{0}</name>
    </namespace>
    {3}
    {4}
    {5}
    <keywords>
        {7}
    </keywords>
    {8}
    {9}
    {10}
    {11}
    <projectionPolicy>FORCE_DECLARED</projectionPolicy>
    <enabled>true</enabled>
    <store class="wmsStore">
        <name>{0}:{1}</name>
    </store>
</wmsLayer>
""".format(
    workspace,
    storename,
    layername,
    "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "",
    "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
    "<description>{}</description>".format(encode_xmltext(parameters.get("description"))) if parameters.get("description") else "",
    "<nativeName>{}</nativeName>".format(parameters.get("native_name")) if parameters.get("native_name") else "",
    os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
    "<nativeCRS>{}</nativeCRS>".format(parameters.get("nativeCRS")) if parameters.get("nativeCRS") else "",
    "<srs>{}</srs>".format(parameters.get("srs")) if parameters.get("srs") else "",
    """
    <nativeBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </nativeBoundingBox>
""".format(*parameters.get("nativeBoundingBox")) if parameters.get("nativeBoundingBox") else "",
    """
    <latLonBoundingBox>
        <minx>{}</minx>
        <maxx>{}</maxx>
        <miny>{}</miny>
        <maxy>{}</maxy>
        <crs>{}</crs>
    </latLonBoundingBox>
""".format(*parameters.get("latLonBoundingBox")) if parameters.get("latLonBoundingBox") else ""
)
    r = func(url, auth=(username, password), headers=contenttype_header("xml"), data=layer_data)
    if r.status_code >= 300:
        raise Exception("Failed to update the wmslayer({}:{}:{}). code = {} , message = {}".format(workspace,storename,layername,r.status_code, r.content))

    logger.debug("Succeed to update the wmslayer({}:{}:{}). ".format(workspace,storename,layername))

def has_layergroup(geoserver_url,username,password,workspace,groupname):
    r = requests.get(layergroup_url(geoserver_url,workspace,groupname),headers=accept_header("html"), auth=(username,password))
    return True if r.status_code == 200 else False

def list_layergroups(geoserver_url,username,password,workspace):
    r = requests.get(layergroups_url(geoserver_url,workspace),headers=accept_header("json"),  auth=(username,password))
    if r.status_code >= 300:
        raise Exception("Failed to list the layergroups in workspace({}:{}). code = {},message = {}".format(workspace,r.status_code, r.content))

    return [g["name"] for g in (r.json().get("layerGroups") or {}).get("layerGroup") or [] ]


def delete_layergroup(geoserver_url,username,password,workspace,groupname):
    if not has_layergroup(geoserver_url,username,password,workspace,groupname):
        logger.debug("The layergroup({}:{}) doesn't exist".format(workspace,groupname))
        return

    r = requests.delete(layergroup_url(geoserver_url,workspace,groupname), auth=(username, password))
    if r.status_code >= 300:
        raise Exception("Failed to delete layergroup({}:{}). code = {} , message = {}".format(workspace,groupname,r.status_code, r.content))

    logger.debug("Succeed to delete the layergroup({}:{})".format(workspace,groupname))

def update_layergroup(geoserver_url,username,password,workspace,groupname,parameters):
    if has_layergroup(geoserver_url,username,password,workspace,groupname):
        func = requests.put
        url = layergroup_url(geoserver_url,workspace,groupname)
        create = False
    else:
        #layer doesn't exist
        func = requests.post
        url = layergroups_url(geoserver_url,workspace)
        create = True

    group_data="""<?xml version="1.0" encoding="UTF-8"?>
<layerGroup>
    <name>{1}</name>
    <mode>SINGLE</mode>
    {2}
    {3}
    <workspace>
        <name>{0}</name>
    </workspace>
    <publishables>
        {5}
    </publishables>
    <keywords>
        {4}
    </keywords>
</layerGroup>
""".format(
    workspace,
    groupname,
    "<title>{}</title>".format(encode_xmltext(parameters.get("title"))) if parameters.get("title") else "",
    "<abstract>{}</abstract>".format(encode_xmltext(parameters.get("abstract"))) if parameters.get("abstract") else "",
    os.linesep.join("<string>{}</string>".format(k) for k in  parameters.get('keywords', [])) if parameters.get('keywords') else "", 
    os.linesep.join("""
        <published type="{}">
            <name>{}:{}</name>
        </published>
""".format("layerGroup" if layer["type"] == "group" else "layer",layer["workspace"],layer["name"]) for layer in parameters.get("layers",{}))
)
    r = func(url, auth=(username, password), headers=contenttype_header("xml"), data=group_data)
    if r.status_code >= 300:
        if r.status_code >= 400 and create == False:
            #update group({0}) failed, try to delete and readd it"
            delete_layergroup(geoserver_url,username,password,workspace,groupname)
            update_layergroup(geoserver_url,username,password,workspace,groupname,parameters)
        else:
            raise Exception("Failed to {} the layergroup({}:{}). code = {} , message = {}".format("create" if create else "update",workspace,groupname,r.status_code, r.content))

    logger.debug("Succeed to {} the layergroup({}:{}). ".format("create" if create else "update",workspace,groupname))

def list_layers(geoserver_url,username,password):
    """
    List a tuple([(workspace,[(datastore,[featuretypes])])],[(workspace,[(wmsstore,[wmslayers])])],[(workspace,[layergroup])])
    """
    featuretypes = []
    wmslayers = []
    layergroups = []
    for w in list_workspaces(geoserver_url,username,password):
        data = (w,[])
        for s in list_datastores(geoserver_url,username,password,w):
            data[1].append((s,[]))
            for l in list_featuretypes(geoserver_url,username,password,w,s):
                data[1][-1][1].append(l)

            if not data[1][-1][1]:
                #no layers in datastore,delete the datastore
                del data[1][-1]

        if data[1]:
            featuretypes.append(data)
            
        data = (w,[])
        for s in list_wmsstores(geoserver_url,username,password,w):
            data[1].append((s,[]))
            for l in list_wmslayers(geoserver_url,username,password,w,s):
                data[1][-1][1].append(l)
            
            if not data[1][-1][1]:
                #no layers in wmsstore,delete the wmsstore
                del data[1][-1]

        if data[1]:
            wmslayers.append(data)

        data = (w,[])
        for l in list_layergroups(geoserver_url,username,password,w):
            data[1].append(l)

        if data[1]:
            #no layergroupss in workspace,delete the workspace
            layergroups.append(data)
         
    return (featuretypes,wmslayers,layergroups)

def layers_diff(geoserver1,geoserver2):
    """
    geoserver1 and geoserver2 are tuple(geoserver_url,username,password)
    Return the layers in geoserver1 but not in geoserver2
    """
    geoserver1_layers = list_layers(*geoserver1)
    geoserver2_layers = list_layers(*geoserver2)
    featuretypes = []
    wmslayers = []
    layergroups = []
    #featuretype difference
    for workspace1,stores1 in geoserver1_layers[0]:
        if not stores1:
            continue
        workspace2,stores2 = next((d for d in geoserver2_layers[0] if d[0] == workspace1),(None,None))
        if not stores2 :
            #workspace1 doesn't exist in geoserver2
            featuretypes.append((workspace1,stores1))
            continue

        data = (workspace1,[])
        for store1,layers1 in stores1:
            if not layers1:
                continue

            store2,layers2 = next((d for d in stores2 if d[0] == store1),(None,None))
            if not layers2:
                #store1 doesn't exist in geoserver2
                data[1].append((store1,layers1))
                continue

            data[1].append((store1,[]))
            for layer1 in layers1:
                if layer1 in layers2:
                    continue
                data[1][-1][1].append(layer1)

            if not data[1][-1][1]:
                #all layers in datastore in geoserver1 exist in geoserver2 too. ,delete the datastore
                del data[1][-1]

        if data[1]:
            featuretypes.append(data)
            

    #wmslayer difference
    for workspace1,stores1 in geoserver1_layers[1]:
        if not stores1:
            continue
        workspace2,stores2 = next((d for d in geoserver2_layers[1] if d[0] == workspace1),(None,None))
        if not stores2 :
            #workspace1 doesn't exist in geoserver2
            wmslayers.append((workspace1,stores1))
            continue

        data = (workspace1,[])
        for store1,layers1 in stores1:
            if not layers1:
                continue
            store2,layers2 = next((d for d in stores2 if d[0] == store1),(None,None))
            if not layers2:
                #store1 doesn't exist in geoserver2
                data[1].append((store1,layers1))
                continue

            data[1].append((store1,[]))
            for layer1 in layers1:
                if layer1 in layers2:
                    continue
                data[1][-1][1].append(layer1)

            if not data[1][-1][1]:
                #all layers in datastore in geoserver1 exist in geoserver2 too. ,delete the datastore
                del data[1][-1]
            
        if data[1]:
            featuretypes.append(data)

    #layergroup difference
    for workspace1,groups1 in geoserver1_layers[2]:
        if not groups1:
            continue
        workspace2,groups2 = next((d for d in geoserver2_layers[2] if d[0] == workspace1),(None,None))
        if not workspace2 :
            #workspace1 doesn't exist in geoserver2
            layergroups.append((workspace1,groups1))
            continue

        for group1 in groups1:
            if group1 in groups2:
                continue
            if not layergroups or layergroups[-1][0] != workspace1:
                layergroups.append((workspace1,[group1]))
            else:
                layergroups[-1][1].append(group1)

    return (featuretypes,wmslayers,layergroups)
                
            

