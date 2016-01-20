import os
import hashlib
import json
import logging
import pytz
from datetime import datetime
import time
from jinja2 import Template

from slave_sync_task import ordered_sync_task_type
from slave_sync_env import SYNC_STATUS_PATH,now,DEFAULT_TIMEZONE

logger = logging.getLogger(__name__)

def date_to_str(d):
    return d.strftime("%Y-%m-%d %H:%M:%S.%f")

def date_from_str(d_str):
    """
    convert a string date to date object;return None if failed
    """
    try:
        return datetime.strptime(d_str,"%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=DEFAULT_TIMEZONE)
    except:
        pass

    return None

class SlaveSyncStatus(object):
    """
    manage sync status information of a sync job
    each sync status is consisted with multiple task status object
    """

    @classmethod
    def get_bitbucket_status(cls):
        if not hasattr(cls,"_bitbucket_status"):
            cls._bitbucket_status = SlaveSyncStatus("bitbucket","sync",str(now()))

        return cls._bitbucket_status

    update = "update"
    remove = "remove"
    _status_objects = []
    _modified = False
    def __init__(self,sync_file,action = "update",file_content = None):
        if file_content:
            #file content is not null, worked in persistent mode
            self._status_file = os.path.join(SYNC_STATUS_PATH,sync_file)
            self._info = None
            self._new = False
            if os.path.isfile(self._status_file):
                #load the status file
                with open(self._status_file,'r') as f:
                    txt = f.read()
                    if txt and txt.strip():
                        self._info = json.loads(txt)
                    else:
                        self._info = {}
            else:
                if not os.path.exists(os.path.dirname(self._status_file)):
                    #dir does not exist, create it.
                    os.makedirs(os.path.dirname(self._status_file))
                self._info = {}

            m = hashlib.md5()
            m.update(file_content)
            md5_hash = m.hexdigest()
            if self._info.get('md5',None) != md5_hash or self._info.get('action',None) != action:
                self._info.clear()
                self._info = {"file":sync_file}
                self._info['md5'] = md5_hash
                self._info['action'] = action
                self._new = True

            self._persistent = True
        else:
            #file content is null, worked in non persistent mode
            self._persistent = False
            self._info = {'action':action}
            self._info = {'file':sync_file}
            self._new = True

        if self._info.get('tasks') is None:
            self._info['tasks'] = {}
        for name,t in self._info.get("tasks").items():
            self._info['tasks'][name] = SlaveSyncTaskStatus(t)

        self._execute_succeed = True

        SlaveSyncStatus._status_objects.append(self)

    @property
    def file(self):
        return s._info["file"]

    @property
    def is_new(self):
        return self._new

    @property
    def tasks(self):
        try:
            return len(self._info['tasks'])
        except:
            return 0

    @property
    def execute_succeed(self):
        return self._execute_succeed
        
    @execute_succeed.setter
    def execute_succeed(self,value):
        self._execute_succeed = bool(value)

    def get_task_status(self,name):
        try:
            return self._info['tasks'][name]
        except:
            self._info['tasks'][name] = SlaveSyncTaskStatus({})
            return self._info['tasks'][name]

    def set_task_status(self,name,task_status):
        if task_status:
            if isinstance(task_status,SlaveSyncTaskStatus):
                self._info['tasks'][name] = task_status
            else:
                self._info['tasks'][name] = SlaveSyncTaskStatus(task_status)

    @property
    def is_succeed(self):
        return all([s.is_succeed for s in self._info.get("tasks",{}).values() if s.run])

    @property
    def is_not_succeed(self):
        return any([s.is_not_succeed  for s in self._info.get("tasks",{}).values() if s.run])
        
    @staticmethod
    def all_succeed():
        """
        Return true, if all files are processed successfully; otherwise return False
        """
        return all([s.is_succeed for s in SlaveSyncStatus._status_objects])

    @staticmethod
    def save_all():
        """
        save all status object into file system
        """
        for s in SlaveSyncStatus._status_objects:
            s.save()

    @staticmethod
    def get_failed_status_objects():
        """
        Return all status object for failed files.
        """
        return [s for s in SlaveSyncStatus._status_objects if s.is_not_succeed]

    @property
    def is_processed(self):
        return any([s.is_processed for s in self._info["tasks"].values()])

    @property
    def file(self):
        """
        Return the associated file
        """
        return self._info.get('file','')

    header_template = Template("""
Sync File : {{task.file}}
    Last Process Time : {{task.last_process_time}}
    Succeed : {{is_succeed}}
""")

    header_template_bitbucket = Template("""
Synchronize file from repository
    Succeed : {{is_succeed}}
""")

    task_header_template_1  = Template("""
    Task {{task_index}} : {{task_type}}
        Succeed : {{task_status.status}}
        Process Time : {{task_status.last_process_time}}
""")
    task_header_template_2  = Template("""
    Task {{task_index}} : {{task_type}}
        Succeed : {{task_status.status}}
        Process Time : {{task_status.last_process_time}}
        {% for key,value in task_status["messages"].iteritems() -%}
        {{key|capitalize}} : {{value}}
        {% endfor -%}
""")

    stage_template_1 = Template("""
        Stage : {{stage_name}}
            Succeed : {{stage_status.status}}
            Process Time : {{stage_status.last_process_time}}
""")
    stage_template_2 = Template("""
        Stage : {{stage_name}}
            Succeed : {{stage_status.status}}
            Process Time : {{stage_status.last_process_time}}
            {% for key,value in stage_status["messages"].iteritems() -%}
            {{key|capitalize}} : {{value}}
            {% endfor -%}
""")

    def __str__(self):
        message = self.header_template.render({"task":self._info,"is_succeed":self.is_succeed})
        task_index = 0
        if self == SlaveSyncStatus.get_bitbucket_status():
            message = self.header_template_bitbucket.render({"task":self._info,"is_succeed":self.is_succeed})
            for task_type,task_status in self._info["tasks"].iteritems():
                task_index += 1
                message += os.linesep + (self.task_header_template_2 if task_status.has_message() else self.task_header_template_1 ).render({"task_index":task_index,"task_type":task_type,"task_status":task_status})
                for stage_name,stage_status in self._info["tasks"][task_type].get("stages",{}).iteritems():
                    message += os.linesep +  (self.stage_template_2 if stage_status.has_message() else self.stage_template_1 ).render({"stage_name":stage_name, "stage_status":stage_status})
           
        else:
            message = self.header_template.render({"task":self._info,"is_succeed":self.is_succeed})
            for task_type in ordered_sync_task_type:
                if task_type not in self._info["tasks"]: continue
                task_status = self._info["tasks"][task_type]
                if not task_status.run: continue
                task_index += 1
                message += os.linesep + (self.task_header_template_2 if task_status.has_message() else self.task_header_template_1 ).render({"task_index":task_index,"task_type":task_type,"task_status":task_status})
                for stage_name,stage_status in self._info["tasks"][task_type].get("stages",{}).iteritems():
                    message += os.linesep +  (self.stage_template_2 if stage_status.has_message() else self.stage_template_1 ).render({"stage_name":stage_name, "stage_status":stage_status})
           
        return message

    def save(self):
        """
        save the status to file
        """
        if self._persistent and self.is_processed:
            self._info['status'] = self.is_succeed
            with open(self._status_file,'w') as f:
                f.write(json.dumps(self._info))

    @property
    def last_process_time(self):
        if "last_process_time" in self._info:
            return date_from_str(self._info["last_process_time"])
        else:
            return None
 
    @last_process_time.setter
    def last_process_time(self,d):
        self._info["last_process_time"] = date_to_str(d)  

class SlaveSyncTaskStatus(dict):
    """
    status object for a task.
    """
    _modified = False
    _run = False
    def __init__(self,task_status={}):
        super(SlaveSyncTaskStatus,self).__init__(task_status)

        #remove failed stages
        for s in self.get("stages",{}).keys():
            if self.is_stage_not_succeed(s):
                del self["stages"][s]

        #if no succeed stages and current task  is not succeed, clear all task status data.
        if not self.get("stages") and self.is_not_succeed:
            self.clear()
                
        #init a messages dictionary object
        if "messages" not in self :
            self["messages"] = {}

    @property
    def run(self):
        return self._run
        
    @run.setter
    def run(self,value):
        self._run = bool(value)

    @property
    def is_processed(self):
        return self._modified

    @property
    def is_succeed(self):
        """
        Return true, if the file is processed successfully; otherwise return False
        """
        return self.get('status',False)

    @property
    def is_not_succeed(self):
        """
        Return true, if the file is processed failed or not executed before; otherwise return False
        """
        return not self.is_succeed

    @property
    def last_process_time(self):
        if "last_process_time" in self:
            return date_from_str(self["last_process_time"])
        else:
            return None
 
    @last_process_time.setter
    def last_process_time(self,d):
        self["last_process_time"] = date_to_str(d)  

    @property
    def shared(self):
        return self.get("shared",False)
    
    @shared.setter
    def shared(self,value):
        self["shared"] = bool(value)

    def failed(self):
        """
        Set a flag indicate this file is processed failed
        """
        self['status'] = False
        self._modified = True
    
    def succeed(self):
        """
        Set a flag indicate this file is processed successfully
        """
        self['status'] = True
        self._modified = True

    def get_message(self,key):
        """
        get the message with key
        """
        try:
            return self["messages"][key]
        except:
            return ""

    def has_message(self):
        return self["messages"]

    def set_message(self,key,message):
        """
        set a message with key
        """
        self["messages"][key] = message
        self._modified = True

    def del_message(self,key):
        """
        delete a message
        """
        try:
            del self["messages"][key]
        except:
            pass
        self._modified = True

    @property
    def all_stages_succeed(self):
        """
        Return True if all stages succeed, or no stages
        """
        return all([s.get('status',False) for s in self.get("stages",{}).values()])

    @property
    def stages(self):
        return self._info.get("stages",{})

    def is_stage_succeed(self,stage):
        """
        Return true, if the stage is processed successfully; otherwise return False
        Return false, if it does not executed.
        """
        try:
            return self["stages"][stage]['status']
        except:
            return False

    def is_stage_not_succeed(self,stage):
        """
        Return true, if the file is processed failed or not executed before; otherwise return False
        """
        return not self.is_stage_succeed(stage)

    def stage_failed(self,stage):
        """
        Set a flag indicate this stage is processed failed
        """
        if "stages" not in self:
            self["stages"] = {}
        if stage not in self["stages"]:
            self["stages"][stage] = {}

        self["stages"][stage]['status'] = False
        self["stages"][stage]['last_process_time'] = date_to_str(now())
        self._modified = True
    
    def stage_succeed(self,stage):
        """
        Set a flag indicate this state is processed successfully
        """
        if "stages" not in self:
            self["stages"] = {}
        if stage not in self["stages"]:
            self["stages"][stage] = {}

        self["stages"][stage]['status'] = True
        self["stages"][stage]['last_process_time'] = date_to_str(now())
        self._modified = True

    def has_stage_message(self,stage):
        try:
            return self["stages"][stage]["messages"]
        except:
            return False
    
    def get_stage_message(self,stage,key):
        """
        get the stage message with key
        """
        try:
            return self["stages"][stage]["messages"][key]
        except:
            return ''

    def set_stage_message(self,stage,key,message):
        """
        set a stage message with key
        """
        if "stages" not in self:
            self["stages"] = {}
        if stage not in self["stages"]:
            self["stages"][stage] = {}
        if "messages" not in self["stages"][stage]:
            self["stages"][stage]["messages"] = {}

        self["stages"][stage]["messages"][key] = message
        self._modified = True

    def del_stage_message(self,stage,key):
        """
        delete a stage message
        """
        try:
            del self["stages"][stage]["messages"][key]
        except:
            pass
        self._modified = True


