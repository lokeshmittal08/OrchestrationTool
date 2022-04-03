

import importlib
from Common.Utilities._utils import _utils
from Common.Logger.logger import logger
import sys
import re
from Common.Logger._Metadata import _Metadata

class pythonexecutor:

    def __init__(self, workflowname):
        self.utilobj = _utils()
        self.workflowname = workflowname
        self.inputval = None
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '

    def pythonexecute(self, task, operationvariable, step_exec_id, workflowname, argsdict=None, parameters=None,
                      result=None, pretask_df=None):
       try:
           if result is None:
               result = []
           self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id))
           self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                             'Input parameter to  ' + __name__ + ' are task :' + str(
                              task) + ' command line argument :' +str(argsdict) + ' result;' + str(result))
           yamlobj = self.utilobj.getfactory('yamlparser')
           metaobj = _Metadata()
           if pretask_df is not None or argsdict is not None:
               operationvariable = metaobj.argsolver(operationvariable, pretask_df, argsdict)
           datasharing = metaobj.configreader('DATASHARING_PATH', 'file_path')

           appcodepath = metaobj.configreader('APPCODE_PATH', 'file_path')
           self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                 step_exec_id) + ' datasharingpath ' + datasharing)
           url = None
           if yamlobj.getvalue(task, 'tasktype.system.executor.method') is not None:
               pyfilename = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.scriptname',
                                                    parameters).spilt('.')[0]
               methodname = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.method',
                                                    parameters)
               loadtype = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.loadtype',
                                                    parameters)
               loadfrequency = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.loadfrequency',
                                                    parameters)
               sys.path.append(appcodepath)
               pathseprator = metaobj.configreader('DELIMITERFORPATH', 'delimiter')
               workflowname = sys.argv[1]
               inilist = [m.start() for m in re.finditer(pathseprator,workflowname)]
               if inilist:
                   loc_start = inilist[-2] + 1
                   loc_end = inilist[-1]
                   project_name = workflowname[loc_start:loc_end]
                   userdefined = 'APP.UserDefinedProcess.' + project_name + '.'
               else:
                   userdefined = 'APP.UserDefinedProcess.'

               mymodule = importlib.import_module(userdefined + pyfilename, pyfilename)
               class_ = getattr(mymodule, pyfilename)
               obj = class_(self.workflowname)
               if argsdict is None:
                   if loadtype is None and loadfrequency is not None:
                       myval = getattr(obj, methodname) (result, step_exec_id, operationvariable)
                   else:
                       myval = getattr(obj, methodname) (result, step_exec_id, operationvariable,
                                                         loadtype, loadfrequency)

               else:
                   if loadtype is None and loadfrequency is None:
                      myval = getattr(obj, methodname) (result, step_exec_id, operationvariable, argsdict)
                   else:
                      myval = getattr(obj, methodname) (result, step_exec_id, operationvariable, loadtype,
                                                        loadfrequency, argsdict)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                              step_exec_id) + ' return value for pyhtonexecute :' + str(myval))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               result['STATUS'] = 'S'
               result['PICKLEFILE'] = myval
               return result

               return myval
       except Exception as e:
               exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
               self.logobj.error(' Error Message :' + str(exec_info))
               raise



