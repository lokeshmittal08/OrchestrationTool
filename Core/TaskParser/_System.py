from Core.TaskParser.pythonexecutor import pythonexecutor
# from Core.TaskParser.httprequester import httprequester
# from Core.TaskParser._Compress import _FileCompress
# from Core.TaskParser._Purge import _FilePurge
# from Core.TaskParser.logprinter import logprinter
from Common.Utilities._utils import _utils
from Common.Logger.logger import logger
from Core.TaskParser.oracleexecutor import oracleexecutor
# from Core.TaskParser._Filechecker import _Filechecker
# from Core.TaskParser.Mailer import Mailer
# from Core.TaskParser.Splitter import  Splitter
# from Core.TaskParser._filemove import _filemove
import traceback

class _System:
    def __init__(self,workflowname):
        self.factobj = _utils()
        self.exeobj= pythonexecutor(workflowname)
        # self.filemoveobj=_filemove(workflowname)
        # self.filecheckerobj = _Filechecker(workflowname)
        # self.mailerobj = Mailer(workflowname)
        # self.splitterobj = Splitter(workflowname)
        # self.httpobj=httprequester(workflowname)
        # self.logprintobj=logprinter(workflowname)
        # self.compressobj = _FileCompress(workflowname)
        self.oracleobje=oracleexecutor(workflowname)
        self.log = logger(workflowname)
        # self.purgeobj = _FilePurge(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '

    def systemtaskdecider(self,task,operationvariable,step_exec_id,workflowname=None,argdict=None,parameters=None,
                          result=None,pretask_df=None, ):
        try:
            self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
            yamlobj = self.factobj.getfactory('yamlparser')
            if yamlobj.getvalue(task,'tasktype.sysytem.executor') is not None and yamlobj.getvalue\
                        (task,'tasktype.system.executor.operator') in ('python'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                  'task type is : system and operator is : python')
                pyreturn =self.exeobj.pythonexecute(task, operationvariable, step_exec_id, workflowname,
                                                    argdict,parameters, result,pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                          'return value for pythonexecute :' + str(pyreturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return pyreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and yamlobj.getvalue(task,
                                                                                                    'tasktype.system.executor.operator') in ('purge'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : purge')
                purgereturn =self.purgeobj.file_purge(task, operationvariable, step_exec_id, workflowname,
                                                  argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for pythonexecute :' + str(purgereturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return purgereturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and yamlobj.getvalue(task,
                                                                      'tasktype.system.executor.operator') in ('compress'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : compress')
                compressreturn =self.compressobj.file_compress(task, operationvariable, step_exec_id, workflowname,
                                                  argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for compress :' + str(compressreturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return compressreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and \
                          yamlobj.getvalue(task,'tasktype.system.executor.operator') in ('http'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : http')
                httpreturn=self.httpobj.httprequest(task, operationvariable, step_exec_id, workflowname,
                                                  argdict, parameters, result)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httpreturn :' + str(httpreturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return httpreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and\
                      yamlobj.getvalue(task, 'tasktype.system.executor.operator') in ('Mail'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : Mail')
                mailreturn=self.mailerobj.mailer(task, operationvariable, step_exec_id, workflowname,
                                                  argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httprequest :' + str(mailreturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return mailreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and \
                       yamlobj.getvalue(task, 'tasktype.system.executor.operator') in ('Mailer'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                            'task type is : system and operator is : Mail')
                mailreturn=self.mailerobj.mail(task, operationvariable, step_exec_id, workflowname,
                                       argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                      'return value for httprequest :' + str(mailreturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return mailreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and \
                     yamlobj.getvalue(task, 'tasktype.system.executor.operator') in ('Splitter'):
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : Splitter')
               splitreturn=self.splitterobj.split_file(task, operationvariable, step_exec_id, workflowname,
                                               argdict, parameters, result, pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httprequest :' + str(splitreturn))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return splitreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and yamlobj.getvalue(task,
                                        'tasktype.system.executor.operator') in (
                    'move'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : log')
                filemovereturn = self.filemoveobj.movefile(task, operationvariable, step_exec_id, workflowname,
                                               argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httprequest :' + str(filemovereturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return filemovereturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and yamlobj.getvalue(task,
                                 'tasktype.system.executor.operator') in (
                    'check'):
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                      'task type is : system and operator is : filechecking')
               filecheckerreturn = self.filecheckerobj.checkfile(task, operationvariable, step_exec_id, workflowname,
                                       argdict, parameters, result, pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                      'return value for httprequest :' + str(filecheckerreturn))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return filecheckerreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and \
                       yamlobj.getvalue(task, 'tasktype.system.executor.operator') in ('log'):
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : log')
               logreturn = self.logprintobj.logprint(task, operationvariable, step_exec_id, workflowname,
                                               argdict, parameters, result, pretask_df)
               self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httprequest :' + str(logreturn))
               self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
               return logreturn
            if yamlobj.getvalue(task, 'tasktype.sysytem.executor') is not None and \
                       yamlobj.getvalue(task, 'tasktype.system.executor.operator') in ('oracle'):
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'task type is : system and operator is : oracle')
                oraclereturn = self.oracleobje.oracleexecute(task, operationvariable, step_exec_id, workflowname,
                                               argdict, parameters, result, pretask_df)
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                              'return value for httprequest :' + str(oraclereturn))
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return oraclereturn
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise





