from Core.TaskParser._Csvparser import _Csvparser
from Common.Utilities._utils import _utils
from Common.Logger.logger import logger

from Core.TaskParser._Oracleparser import  _Oracleparser
# from Core.TaskParser._Sftpparser import  _Sftpparser

from Core.TaskParser._Excelparser import _Excelparser
from Core.TaskParser._Mqparser import _mgparser
from Core.TaskParser._Xmlparser import  _Xmlparser
# from Core.TaskParser._Jsonparser import  _Jsonparser
import traceback

class _source:

     def __init__(self,workflowname):
         self.factobj = _utils()
         self.oracleparser=_Oracleparser(workflowname)
         self.csvparser=_Csvparser(workflowname)
         self.excelparser=_Excelparser(workflowname)
         self.mqparser=_mgparser(workflowname)
         self.xmlparser=_Xmlparser(workflowname)
         # self.jsonparser=_Jsonparser(workflowname)
         self.log = logger(workflowname)
         self.logobj = self.log.getcustomlogger()
         self.step_exec_msg = ' step_exec_id : '
         self.step_start_msg = 'Started execution of : '
         self.step_end_msg = ' Ended execution of : '


     def sourcedecider(self,task,operationvariable,step_exec_id,pretask_df=None,workflowname=None,argdict=None,parameters=None):
         try:
             # sftpparseobj = _Sftpparser(workflowname)
             self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id))
             yamlobj=self.factobj.getfactory('yamlparser')
             if yamlobj.getvalue(task,'source.db') is not None and yamlobj.getvalue(task,'source.db.dbtype')=='oracle' :
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                   'source task type is : db and dbtype is : oracle')
                 parserval=self.oracleparser.fetchdata(task, operationvariable, step_exec_id, pretask_df,
                                                       workflowname,argdict,parameters)
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                  'return value of fetchdata :'+str(parserval))
                 self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                 return parserval
             if yamlobj.getvalue(task,'source.file') is not None and yamlobj.getvalue(task,'source.file.filetype')=='csv' :
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                   'source task type is : file and dbtype is : csv')
                csvreturnval = self.csvparser.readfile(task, operationvariable, step_exec_id,
                                                       workflowname,argdict,parameters,pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                  'return value of fetchdata :'+str(csvreturnval))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                return csvreturnval
             if yamlobj.getvalue(task,'source.file') is not None and ( yamlobj.getvalue(task,'source.file.filetype')=='xls' or yamlobj.getvalue(task,'source.file.filetype')) =='xlsx':
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                  'source task type is : file and dbtype is : xls')
                excelreturnval =self.excelparser.readfile(task, operationvariable, step_exec_id,
                                                          workflowname,argdict,parameters,pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                  'return value of fetchdata :'+str(excelreturnval))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                return excelreturnval
             if yamlobj.getvalue(task, 'source.operation') is not None and yamlobj.getvalue(task,'source.operation.operationtype') \
                 == 'sftp':
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                   'source task type is : file and dbtype is : sftp')
                 # sftpreturnval =sftpparseobj.Sftpget(task, operationvariable, step_exec_id,workflowname, argdict, parameters, pretask_df)
                 # self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                 #                    'return value of fetchdata :' + str(sftpreturnval))
                 # self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                 # return sftpreturnval
             if yamlobj.getvalue(task, 'source.queue') is not None and yamlobj.getvalue(task,'source.queue.queuetype')=='mq' :
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                   'source task type is : file and dbtype is : mq')
                 mqparserval=self.mqparser.readmq(task, operationvariable, step_exec_id,workflowname, argdict, parameters, pretask_df)
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                    'return value of fetchdata :' + str(mqparserval))
                 self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                 return mqparserval
             if yamlobj.getvalue(task,'source.file') is not None and yamlobj.getvalue(task,'source.file.type')=='json' :
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                   'source task type is : file and dbtype is : json')
                 jsonparserval=self.jsonparser.readparser(task, operationvariable, step_exec_id,
                                                          workflowname, argdict, parameters, pretask_df)
                 self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                    'return value of fetchdata :' + str(jsonparserval))
                 self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                 return jsonparserval
         except Exception as e:
                exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                self.logobj.error(' Error Message : ' + str(exec_info))
                raise



