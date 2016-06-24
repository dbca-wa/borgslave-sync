from geoserver.resource import FeatureType
from geoserver.catalog import Catalog
from geoserver.support import url
 

def publish_featuretype(self, name, store, native_crs, srs=None,keywords=None,title=None,abstract=None,nativeName=None,jdbc_virtual_table=None):
    '''Publish a featuretype from data in an existing store'''
    # @todo native_srs doesn't seem to get detected, even when in the DB
    # metadata (at least for postgis in geometry_columns) and then there
    # will be a misconfigured layer
    if native_crs is None: raise ValueError("must specify native_crs")
    if native_crs == "EPSG:0": raise ValueError("CRS was set as EPSG:0! Check that the Input object is specified with a valid CRS")
    srs = srs or native_crs
    feature_type = FeatureType(self, store.workspace, store, name)
    # because name is the in FeatureType base class, work around that
    # and hack in these others that don't have xml properties
    feature_type.dirty['name'] = name
    feature_type.dirty['srs'] = srs
    feature_type.dirty['nativeCRS'] = native_crs
    if nativeName:
        feature_type.dirty["nativeName"] = nativeName
    else:
        feature_type.dirty["nativeName"] = name

    if title:
        feature_type.dirty['title'] = title
    else:
        feature_type.title = name
    if abstract:
        feature_type.dirty['abstract'] = abstract
    if keywords:
        feature_type.dirty['keywords'] = keywords
    feature_type.enabled = True
    headers = {
        "Content-type": "application/xml",
        "Accept": "application/xml"
    }
    
    resource_url=store.resource_url
    if jdbc_virtual_table is not None:
        feature_type.metadata=({'JDBC_VIRTUAL_TABLE':jdbc_virtual_table})
        params = dict()
        resource_url=url(self.service_url,
            ["workspaces", store.workspace.name, "datastores", store.name, "featuretypes.json"], params)
    
    headers, response = self.http.request(resource_url, "POST", feature_type.message(), headers)
    feature_type.fetch()
    return feature_type

Catalog.publish_featuretype = publish_featuretype
