from Common.Logger._Metadata import _Metadata
from Common.Utilities._utils import _utils
from Common.Logger.logger import logger
import os


import traceback
import datetime
import time

class _filechecker:

    def __init__(self,workflowname):
        self.utilobj=_utils()
        self.inputval=None
        self.metaobj = _Metadata()
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg='Workflow_exec_id :'
        self.step_start_msg='Started execution of :'
        self.step_end_msg = 'Ended execution of :'

    def checkfile(self,task,operationvariable,workflow_exec_id,workflowname,argdict=None,parameters=None,result=None,pretask_df=None):
           try:
               result={}
               running=True
               self.logobj.info(self.step_exec_msg + str(workflow_exec_id) + self.step_start_msg + __name__)
               yamlobj = self.utilobj.getfactory('yamlparser')
               if pretask_df is not None or argdict is not None:
                   operationvariable=self.metaobj.argsolver( operationvariable, pretask_df, argdict)
               folderpath = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executer.folderpath',parameters,argdict)
               pattern = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executer.pattern', parameters,argdict)
               schedulestopper = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executer.schedulestopper',
                                                         parameters,argdict)
               if self.metaobj.configreader(folderpath, 'file_path') is not None:
                   folderpath = self.metaobj.configreader(folderpath, 'file_path')
               if self.metaobj.configreader(schedulestopper, 'schedulestoptime') is not  None:
                   scheduletime = self.metaobj.configreader(schedulestopper, 'schedulestoptime')
                   window = self.metaobj.configreader(schedulestopper, 'stopwindow')
               while running:
                   filenames = self.metaobj.get_file(folderpath, 'ALL',pattern)
                   if len(filenames)>0:
                       running=False
                       result['STATUS']='S'
                   else:
                       time.sleep(15)

                   currentdatetime = datetime.datetime.now()
                   currenthour = currentdatetime.hour
                   currentminutes = currentdatetime.minute
                   currentsecond = currentdatetime.second
                   if int(currenthour) == int(scheduletime.spilt(';')[0]) and (
                              int(currentminutes) >= int(scheduletime.spilt(':') [1]) and int(currentminutes) < int(
                      scheduletime.spilt(':')[1]) + int(window)):
                      running = False
                      result['STATUS']='W'
               if not running:
                   return result
               self.logobj.info(self.step_exec_msg + str(workflow_exec_id) + self.step_end_msg + __name__)
               return result
           except Exception as e:
               exc_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
               self.logobj.error(' Error Message : ' + str(exc_info))
               raise








