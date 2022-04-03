import yaml

from Common.Utilities._utils import _utils
import traceback


class _Taskloader:
    def __init__(self):
        self.my_dict=[]


    def loadtask(self,taskname,workflowname,template=None, applicationname=None):
        try:
            factobj = _utils()
            configreaderobj = factobj.getfactory('configreader')
            seprator = configreaderobj.configSectionMap('DELIMITERFORPATH')
            appfileseprator = str(seprator['delimiter'])
            if templatefile=='Y':
                filepath = configreaderobj.configSectionMap('TEMPLATE_PATH')
                appfilepath = str(filepath['file_path'])
                taskname= appfilepath + appfileseprator + taskname+' .yaml'
            else:
                filepath = configreaderobj.configSectionMap('CONFIG_PATH')
                appfilepath = str(filepath['file_path'])
                taskname = appfilepath  + applicationname + appfileseprator + workflowname + appfileseprator + taskname + '.yaml'
            self.my_dict = yaml.load(open(taskname))
            return self.my_dict
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message :' + str(exec_info))
            

