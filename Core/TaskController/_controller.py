from Common.AuditProcess.Auditordecorator import Auditordecorator
from Core.TaskParser._Task import _Task
from Core.TaskParser._stepreader import _stepreader
from Common.Logger._Metadata import _Metadata
from Common.Logger.logger import logger
import traceback

class _controller:

    def __init__(self,workflowname):
        self.val=None
        self.taskparseobj=_Task(workflowname)
        self.steppreaderobj=_stepreader(workflowname)
        self.rowcount=0
        self.audit_dict = ()
        self.target_audit_dict=()
        self.metaobj = _Metadata(workflowname)
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '




        @Auditordecorator()
        def taskauditing(self,sourcetask,auditdict,auditflag,operationvariable,step_exec_id,pretask_df=None,workflowname=None,
                         argsdict=None,parameters=None,inputdata=None):
            return self.taskparseobj.taskdecider(sourcetask,operationvariable,step_exec_id,pretask_df,workflowname,
                                                 argsdict,parameters,inputdata)


        @Auditordecorator()
        def readerauditing(self,sourcetask,auditdict,auditflag,operationvariable,step_exec_id):
            return None

        @Auditordecorator()
        def taskauditing(self,targettask,auditdict,auditflag,operationvariable,step_exec_id,pretask_df=None,workflowname=None,
                         argsdict=None,parameters=None,inputdata=None,task_exec_id=None):
            return self.taskparseobj.taskdecider(targettask,operationvariable,step_exec_id,pretask_df,workflowname,
                                                 argsdict,parameters,inputdata)


        def parse(self,task,step_exec_id,workflow,argsdict,application_id, application_name ):
            try:
                total_no_of_rows=0
                self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +' are task :'+str(task))
                operationvariables,pretask_dict,sourcetask,steptask,targettask,posttask =self.metaobj.steptptasksplit(task)
                self.logobj.debug(self.step_exec_msg + str(step_exec_id) + ' returned value after calling workflowtotasksplit '
             'are operationvariables: ' + str(operationvariables) + ' pretask_dict : '+ str(pretask_dict)+'sourcetask : '
                                  +str(sourcetask)+'targettask : '+str(targettask)+'posttask :'+str(posttask))




                pretask_df=None
                try:
                    if self.metaobj.taskvalue(task, 'pretask') is not None:
                        for iter in self.metaobj.gettaskvalue(task, 'pretask'):
                            pretask_df1 = self.metaobj.taskchecker(iter, 'tasktype', workflowname, application_name)
                            pretask_audit_dict=self.metaobj.taskauditcreator(pretask_df1['name'], step_exec_id, None, application_id)
                            for i in range(pretask_df1['tasktype'].__len__()):
                                pretask_dict_val=pretask_df1['tasktype'][i]
                                pretask_audit_dict['recordcount'] = 0
                                pretask_audit_dict['sourceidentifier']=None
                                if '(' in iter['name']:
                                    resultout, task_exec_id = self.taskauditing(pretask_dict_val, pretask_audit_dict, 'RU',
                                                                                operationvariables, step_exec_id, pretask_df,
                                                                                workflowname,argsdict,iter['name'])
                                else:
                                    resultout, task_exec_id = self.taskauditing(pretask_dict_val, pretask_audit_dict, 'RU',
                                                      operationvariables, step_exec_id, pretask_df,
                                                            workflowname, argsdict)

                                pretask_audit_dict['task_exec_id'] = task_exec_id
                                pretask_audit_dict['recordcount'] = resultout['recordcount']
                                pretask_df=resultout['picklefile']
                except Exception as e:
                    exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.pretask_audit_dict['message'] = exec_info
                    self.readerauditing(pretask_dict_val, pretask_audit_dict, 'E', operationvariables)
                    self.logobj.error(' Error Message :' + str(exec_info))
                    raise

                try:
                    pickledict=[]
                    if self.metaobj.taskvalue(task, 'Reader') is not None:
                        for iter in self.metaobj.gettaskvalue(task, 'Reader'):
                            sourcetask1 = self.metaobj.taskchecker(iter, 'tasktype', workflowname, application_name)
                            self.audit_dict = self.metaobj.taskauditcreator(sourcetask1['name'], step_exec_id, None, application_id)
                            self.audit_dict['recordCount'] = 0
                            self.audit_dict['FirstRun'] = 'Y'
                            for i in range(sourcetask1['tasktype'].__len__()):
                                sourcetask_val = sourcetask1['tasktype'][i]
                                if '(' in iter['name']:
                                    self.audit_dict['sourceidentifier'] = self.metaobj.sourceidentifierfinder(
                                           sourcetask_val, operationvariables, 'source',iter['name'])
                                    self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                            step_exec_id) + ' sourceidentifier is : ' + self.audit_dict['sourceidentifier'])
                                    resultout, task_exec_id = self.taskauditing(sourcetask_val, self.audit_dict, 'RU',
                                                                                operationvariables, step_exec_id, pretask_df,
                                                                                 workflowname,argsdict,iter['name'])
                                else:
                                     self.audit_dict['sourceidentifier'] = self.metaobj.sourceidentifierfinder(
                                               sourcetask_val, operationvariables, 'source')
                                     self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                          step_exec_id) + ' sourceidentifier is : ' + self.audit_dict['sourceidentifier'])
                                     resultout, task_exec_id = self.taskauditing(sourcetask_val,self.audit_dict,'RU',
                                                                                operationvariables,step_exec_id,pretask_df,
                                                                                workflowname,argsdict)
                                self.audit_dict['task_exec_id']=task_exec_id
                                if resultout is not None:
                                    self.audit_dict['recordCount'] = resultout['recordcount']
                                    total_no_of_rows = total_no_of_rows + self.audit_dict['recordCount']
                                    pickledict[self.audit_dict['sourceidentifier']] = resultout['picklefile']
                                else:
                                    self.audit_dict['recordCount'] = 0
                        self.logobj.debug(' Executing method : '+__name__+self.step_exec_msg + str(step_exec_id) +
                                           ' result after calling source task : ' + str(pickledict))
                except Exception as e:
                    exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.pretask_audit_dict['message'] = exec_info
                    self.readerauditing(sourcetask_val, self.audit_dict, 'E', operationvariables)
                    self.logobj.error(' Error Message :' + str(exec_info))
                    raise

                try:
                    out=None
                    self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                        step_exec_id) + 'parameter for workflowreader are steptask : ' + str(pickledict) + ' workflowname ;' + str(
                        workflowname))
                    if self.metaobj.taskvalue(task, 'processor') is not None :
                        for iter in self.metaobj.gettaskvalue(task, 'processor'):
                            if out is not None:
                                pickledict=out
                            steptask = self.metaobj.taskchecker(iter, 'tasks', workflowname, application_name)
                            steptask['FirstRun'] = 'Y'
                            steptask['recordCount']=total_no_of_rows
                            out = self.stepreaderobj.stepread(steptask, operationvariables, step_exec_id, pickledict,
                                        workflowname,argsdict,iter['name'],pretask_df, application_id, application_name)
                            self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                              step_exec_id) + ' return value after calling workflowread : ' + str(out))
                except Exception as e:
                    exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.audit_dict['message'] = exec_info
                    self.logobj.error(' Error Message :' + str(exec_info))
                    raise

                try:
                    if self.metaobj.taskvalue(task, 'Writer') is not None:
                       for iter in self.metaobj.gettaskvalue(task, 'Writer'):
                            targettask1 = self.metaobj.taskchecker(iter, 'tasktype', workflowname, application_name)
                            self.target_audit_dict = self.metaobj.taskauditcreator(targettask['name'], step_exec_id, None, application_id)
                            self.target_audit_dict['task_exec_id'] = 0
                            self.target_audit_dict['task_name'] = targettask['name']
                            for i in range(targettask['tasktype'].__len__()):
                                targettask_val =targettask1['tasktype'][i]
                                if '(' in iter['name']:
                                    self.target_audit_dict['inputsource'] = self.metaobj.sourceidentifierfinder(
                                         targettask_val, operationvariables, 'target', iter['name'])
                                    self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                               step_exec_id) + ' inputsource is : ' + self.target_audit_dict['inputsource'])
                                    self.rowcount, self.target_audit_dict['task_exec_id'] = self.target_audit_dict(targettask_val,
                                                                self.target_audit_dict, 'WU',operationvariables,step_exec_id,pretask_df,
                                                                      workflowname,argsdict,iter['name'],  out)
                                else:
                                    self.target_audit_dict['inputsource'] = self.metaobj.sourceidentifierfinder(
                                                  targettask_val, operationvariables, 'target')
                                    self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                                         step_exec_id) + ' inputsource is : ' + self.target_audit_dict['inputsource'])
                                    self.rowcount, self.target_audit_dict['task_exec_id'] = self.writerauditing(targettask_val,
                                                                    self.target_audit_dict, 'WU',
                                                    operationvariables,step_exec_id, pretask_df,workflowname,argsdict,None, out)
                                self.target_audit_dict['recordCount']=self.rowcount
                                self.logobj.debug(self.step_exec_msg + str(step_exec_id) + ' return output is : ' + str(self.rowcount))
                except Exception as e:
                    exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.target_audit_dict['message'] = exec_info
                    self.readerauditing(targettask_val, self.target_audit_dict, 'E', operationvariables,argsdict)
                    self.logobj.error(' Error Message : ' + str(exec_info))
                    raise

                try:
                    if self.metaobj.taskvalue(task, 'Posttask') is not None:
                        for iter in self.metaobj.gettaskvalue(task, 'Posttask'):
                             posttask = self.metaobj.taskchecker(iter, 'tasks', workflowname, application_name)
                             posttask['recordCount']=0
                             self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                    step_exec_id) +' parameter for workflowreader are steptask : ' + str(posttask)
                                    + ' operationvariables : ' + str(operationvariables) + ' workflowname : ' + str(workflowname))
                             out = self.stepreaderobj.stepread(posttask, operationvariables, step_exec_id, pickledict,
                                        workflowname,argsdict,iter['name'],pretask_df, application_id, application_name)
                             self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                                              step_exec_id) + ' return value after calling posttask : ' + str(out))
                except Exception as e:
                    exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.audit_dict['message'] = exec_info
                    self.readerauditing(sourcetask, self.audit_dict, 'E', operationvariables,argsdict)
                    self.logobj.error(' Error Message :' + str(exec_info))
                    raise

            except Exception as e:
               exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
               self.audit_dict['message'] = exec_info
               self.logobj.error(' Error Message :' + str(exec_info))
               raise
               













