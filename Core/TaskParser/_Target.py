from Core.TaskParser._Oracleparser import _Oracleparser
from Common.Utilities._utils import _utils
from Core.TaskParser._Csvparser import _Csvparser
# from Core.TaskParser._Sftpparser import _Sftpparser
from Common.Logger.logger import logger
from Core.TaskParser._Excelparser import _Excelparser
from Core.TaskParser._Xmlparser import _Xmlparser
from Core.TaskParser._Mqparser import _mgparser
import traceback

class _Target:
    def __init__(self,workflowname):
        self.factobj = _utils()
        self.oracleparser=_Oracleparser(workflowname)
        self.csvparser=_Csvparser(workflowname)
        self.excelparser = _Excelparser(workflowname)
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.xmlarser = _Xmlparser(workflowname)
        self.mqparser = _mgparser(workflowname)
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '

    def targetdecider(self,task,operationvariable,step_exec_id,workflowname,result,argdict=None,parameters=None,pretask_df=None):
        try:
            self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id))
            # sftpparserobj=_Sftpparser(workflowname)
            yamlobj = self.factobj.getfactory('yamlparser')
            if yamlobj.getvalue(task,'target.db') is not None and yamlobj.getvalue(task,'target.db.dbtype')=='oracle' :
               fetchobj = self.oracleparser.loaddata(task, operationvariable,step_exec_id,result, workflowname,
                                                  argdict,parameters,pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : db and dbtype is : oracle')
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return fetchobj
            if yamlobj.getvalue(task,'target.file') is not None and yamlobj.getvalue(task,'target.file.filetype')=='csv':
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : file and dbtype is : csv')
               csvout=self.csvparser.writefile(task, operationvariable, step_exec_id, result, workflowname,
                                                  argdict, parameters, pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value of writefile is :'+str(csvout))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return csvout
            if yamlobj.getvalue(task, 'target.file') is not None and yamlobj.getvalue(task,'target.file.filetype') == 'xml':
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : file and dbtype is : xml')
               xmlout=self.xmlarser.writefile(task, operationvariable, step_exec_id, result, workflowname,
                                              argdict, parameters, pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value of writefile is :' + str(xmlout))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return xmlout
            if yamlobj.getvalue(task,'target.queue') is not None and yamlobj.getvalue(task,'target.queue.queuetype') == 'mq':
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : file and dbtype is : mq')
                mqout = self.mqparser.writemq(task, operationvariable, step_exec_id, result, workflowname,
                                              argdict, parameters, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value of writefile is :' + str(mqout))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return mqout
            if yamlobj.getvalue(task,'target.file') is not None and  (yamlobj.getvalue(task,'target.file.filetype')=='xlsx'
                                                                   or yamlobj.getvalue(task,'target.file.filetype')=='xls'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +'task type is : file' 
                                                                                                              'and dbtype is : csv')
                xlsout=self.excelparser.writefile(task, operationvariable, step_exec_id, result, workflowname,
                                          argdict, parameters, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value of writefile is :' + str(xlsout))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return xlsout
            if yamlobj.getvalue(task,'target.operation') is not None and yamlobj.getvalue(task,'target.operation.operationtype')=='sftp':
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : operation and dbtype is : sftp')
                # sftpout=sftpparserobj.Sftpput(task, operationvariable, step_exec_id, result, workflowname,
                #                           argdict, parameters)
                # self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                #               'return value of Sftpput is :' + str(sftpout))
                # self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                # return sftpout
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise