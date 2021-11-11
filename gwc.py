import os
import json
import time
import requests
from xml.etree import ElementTree
from jinja2 import Template,Environment,FileSystemLoader
import slave_sync_env as settings

class GeoWebCache(object):
    """
    A class provides the api to manipulate cached layer
    """
    def __init__(self,geoserver_url,user,password):
        self._rest_url = "/".join([geoserver_url,"gwc/rest"])
        self._layer_list_url = "/".join([self._rest_url,"layers.xml"])
        self._user = user
        self._password = password
        pass

    def get_layer_url(self,workspace,layer_name,f="xml"):
        """
        return the url of the cached layer
        """
        return "{0}/layers/{1}:{2}.{3}".format(self._rest_url,workspace,layer_name,f)

    def del_layer(self,workspace,layer_name):
        resp = requests.delete(self.get_layer_url(workspace,layer_name), auth=(self._user, self._password))
        
        if resp.status_code >= 400:
            raise Exception("Delete cached layer list failed ({0}: {1})".format(resp.status_code, resp.content))

    def update_layer(self,layer):
        if self.get_layer(layer['workspace'],layer['name']):
            #layer exist, update
            http_method = requests.post
        else:
            #layer not exist, add
            http_method = requests.put

        template = settings.template_env.get_template('gwc_layer_setting.xml')
        resp = http_method(self.get_layer_url(layer['workspace'],layer['name']), auth=(self._user, self._password), headers={'content-type':'text/xml'}, data=template.render(layer))
        
        if resp.status_code >= 400:
            raise Exception("Update cached layer failed. ({0}: {1})".format(resp.status_code, resp.content))

        if not self.get_layer(layer['workspace'],layer['name']):
            raise Exception("Update cached layer failed. ({0}: {1})".format(resp.status_code, resp.content))

    def get_layer(self,workspace,layer_name):
        """
        Get layer definition with xml format
        """
        resp = requests.get(self.get_layer_url(workspace,layer_name), auth=(self._user, self._password))
        if resp.status_code >= 400:
            return None
            #raise Exception("Retrieve cached layer failed. ({0}: {1})".format(resp.status_code, resp.content))
        try:
            root = ElementTree.fromstring(resp.content)
            return resp.content
        except:
            #layer not exist
            return None

    def empty_layer(self,layer):
        http_method = requests.post

        url = "{0}/seed/{1}:{2}.xml".format(self._rest_url,layer["workspace"],layer["name"])
        #launch the truncate task
        template = settings.template_env.get_template('gwc_layer_empty.xml')
        for gridset in ("gda94","mercator"):
            for f in ("image/png","image/jpeg"):
                data = template.render({'layer':layer,'format':f,'gridset':gridset})
                resp = http_method(url, auth=(self._user, self._password), headers={'content-type':'text/xml'}, data=data)
        
                if resp.status_code >= 400:
                    raise Exception("Empty gwc failed. ({0}: {1})".format(resp.status_code, resp.content))
        #check whether the task is finished or not.
        finished = False
        while(finished):
            finished = True
            resp = requests.get(self.get_layer_url(layer['workspace'],layer['name']), auth=(self._user, self._password), headers={'content-type':'application/json'})
            if resp.status_code >= 400:
                raise Exception("Empty gwc failed. ({0}: {1})".format(resp.status_code, resp.content))

            tasks=json.loads(resp.content).get("long-array-array",[])
            for t in tasks:
                if t[3] == -1:
                    #aborted
                    raise Exception("Some task is aborted.")
                elif t[3] in (0,1):
                    finished = False
                    break
            if not finished:
                time.sleep(1)


    @property
    def layers(self):
        resp = requests.get(self._layer_list_url, auth=(self._user, self._password))
        if resp.status_code >= 400:
            raise Exception("Retrieve cached layer list failed. ({0}: {1})".format(resp.status_code, resp.content))
        try:
            root = ElementTree.fromstring(resp.content)
            layers = {}
            ns = {"atom":"http://www.w3.org/2005/Atom"}
            for layer in root:
                el = layer.find("name")
                if el is not None and el.text:
                   layers[el.text] = layer.find("atom:link",ns).get('href')

            return layers
        except:
            raise Exception("Retrieve cached layer list failed. {0}".format(resp.content))

if __name__ == "__main__":
    gwc = GeoWebCache(settings.GEOSERVER_URL[0],settings.GEOSERVER_USERNAME[0],settings.GEOSERVER_PASSWORD[0])
    print(gwc.layers)
