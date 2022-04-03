from Core.Factory.factory import factory
from Common.Utilities._utils import _utils
from Common.Logger.logger import logger
import traceback

class _Task:
    def __init__(self,workflowname=None):
       self.factobj = factory(workflowname)
       self.utils=_utils()
       self.log = logger(workflowname)
       self.logobj = self.log.getcustomlogger()
       self.step_exec_msg = ' Workflow_exec_id : '
       self.step_start_msg = 'Started execution of : '
       self.step_end_msg = ' Ended execution of : '

    def targetdecider(self,task,operationvariable,step_exec_id,pretask_df=None,workflowname=None,argdict=None,parameters=None,result=None):
        try:
            self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
            yamlobj = self.factobj.getfactory('yamlparser')
            if yamlobj.getvalue(task,'source') is not None :
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +'task type is : source')
               sourceobj=self.factobj.getfactory('source')
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
               return sourceobj.sourcedecider(task,operationvariable,step_exec_id,pretask_df,workflowname,argdict,parameters)
               self.logobj.debug(' Executing method : ' + __name__ +self.step_exec_msg + str(step_exec_id) + 'task type is : system')
               systemobj = self.factobj.getfactory('system')
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
               if result is not None:
                  return systemobj.systemtaskdecider(task,operationvariable,step_exec_id,workflowname,argdict,parameters,
                                                   result,pretask_df)
               else:
                  return systemobj.systemtaskdecider(task,operationvariable,step_exec_id,workflowname,argdict,parameters,pretask_df)
            if yamlobj.getvalue(task,'target') is not None:
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) + 'task type is : target')
               targetobj = self.factobj.getfactory('target')
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return targetobj.sourcedecider(task,operationvariable,step_exec_id,workflowname,result,argdict,parameters,pretask_df)
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise


