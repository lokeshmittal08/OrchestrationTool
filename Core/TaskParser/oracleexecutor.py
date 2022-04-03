import importlib
from Common.Logger._Metadata import _Metadata


from Common.Utilities._utils import _utils
from Common.Logger.logger import logger

class oracleexecutor:

     def __init__(self, workflowname):
        self.factobj = _utils()
        self.workflowname=workflowname
        self.inputval=None
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '

     def taskconfigreader(self,task,operationvariable,parameters=None):
         yamlobj = self.utilobj.getfactory('yamlparser')
         configcode=yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.configcode',parameters)
         script = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.script',parameters)
         configkey = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.configkey',parameters)
         configtable = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.configtable',parameters)
         configconnection = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.configconnection',parameters)
         refrenceconnection = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.refrenceconnection',parameters)
         referencecolumn = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.refrencecolumn',parameters)
         refrencetable = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.refrencetable',parameters)
         aggregatemethod = yamlobj.variableimputer(task, operationvariable, 'tasktype.system.executor.aggregatemethod',parameters)
         return configcode,script,configkey,configtable,configconnection,refrenceconnection,referencecolumn,refrencetable\
             ,aggregatemethod

     def oracleexecute(self,task,operationvariable,workflow_exec_id,workflowname,argsdict=None,parameters=None,result=None):
         self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(workflow_exec_id))
         self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(workflow_exec_id) +
                           ' Input parameter for taskconfigreader are task :' + str(task) + ' operationvariable :' + str(
             operationvariable)
                           +' Command Line arguments :'+str(argsdict))
         yamlobj = self.factobj.getfactory('yamlparser')
         configcode,script, configkey, configtable, configconnection, refrenceconnection ,referencecolumn, refrencetable\
         ,aggregatemethod=self.taskconfigreader(task,operationvariable,parameters)
         self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(workflow_exec_id) +
                           ' return value for task config reader is configcode:' + str(configcode)+'configkey : '+
                           str(configkey)+' configtable: '+str(configtable) +' configconnection: '+str(configconnection)+
                          'refrenceconnection: '+str(refrenceconnection) +' refrencecolumn: '+str(referencecolumn)+
                          'refrencetable: '+str(refrencetable)+' aggregatemethod: '+ str(aggregatemethod))
         configobj = self.utilobj.getfactory('oracle', configconnection)
         if script is not None:
             noofcommit = configobj.execute(str(script))
         else:
             refobj = self.utilobj.getfactory('oracle', refrenceconnection)
             sql='select '+aggregatemethod+'('+referencecolumn+') from '+refrencetable
             returndf = refobj.dbselectaall( sql)
             updatesql = 'update '+configtable+' set config_value =:confvalue where config_cd=:conf_code and config_key=:confkey'
             updateparams=(str(returndf.iloc[0,0]),configcode,configkey)
             noofcommit=configobj.execute(str(updatesql),updateparams)
         self.logobj.info(self.step_end_msg + __name__+self.step_exec_msg + str(workflow_exec_id) )
         return noofcommit






