from Common.AuditProcess.Auditordecorator import Auditordecorator
from Core.TaskParser._Task import _Task

from Common.Utilities._utils import _utils
from Common.Logger._Metadata import _Metadata
from Common.Logger.logger import logger
from Common.Utilities.AuditTableCreator import AuditTableCreator
import traceback

class _stepreader:
    def __init__(self,pipelinename):
        self.utilobj=_utils()
        self.taskobj=_Task()
        self.inputparam=None
        self.audit_dict=()
        self.processfile=''
        self.metaobj = _Metadata()
        self.log = logger(pipelinename)
        self.logobj = self.log.getcustomloger()
        self.step_exec_msg='step_exec_id :'
        self.step_start_msg= 'Started execution of :'
        self.step_end_msg = 'Ended execution of :'
        self.task_exec_id=0

    @Auditordecorator()
    def writerauditing(self,task,auditdict,auditflag,operationvariables,step_exec_id,pretask_df=None,pipelinename=None,argsdict=None,
                       parameters=None,inputdata=None,task_exec_id=None):
        self.task_exec_id=task_exec_id
        return self.taskobj.taskdecider(task, operationvariables,step_exec_id,pretask_df,pretask_df,pipelinename,argsdict,parameters,inputdata)

    @Auditordecorator()
    def readerauditing(self,sourcetask,auditdict,auditflag,operationalvariables,task_exec_id):
        return  None

    def stepread(self,steptask,operationalvariables,step_exec_id,result=None,pipelinename=None,argsdict=None,parameter=None,
                pretask_df=None, application_id=None, applicationname=None):
        metaauditentry = AuditTableCreator()
        taskdecider_output=()
        try:
            taskinput=None
            self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id))
            yamlobj = self.utilobj.getfactory('yamlparser')
            if steptask is None:
                dfdict = self.metaobj.pickletodict(result)
                print(dfdict['S1'])
                print('DATA TRANSFORMATION FILE')
                dfdict['T1'] = dfdict['S1']
                picklefile=self.metaobj.dicttopickle(dfdict,step_exec_id)
                return picklefile
            if yamlobj.getvalue(steptask,'tasks.tsk.inputparameter') is not None:
                taskinput=yamlobj.getvalue(steptask,'tasks.task.inputparameter')

            if taskinput == '99DF00' :
                self.inputparam=result
            else:
                print('Non DF input')
                print(taskinput)

            for i in range(steptask['tasks'].__len__()):
                if i > 0:
                    self.inputparam=taskdecider_output
                taskname1 = yamlobj.getvalue(steptask['tasks'][i], 'tasks.name')
                metaauditentry.metatask(taskname1,application_id)
                my_dict=self.metaobj.taskchecker(steptask['tasks'][i], 'tasktype',pipelinename,applicationname)
                self.audit_dict['task_name']=taskname1
                self.audit_dict['step_exec_id']=step_exec_id
                self.audit_dict['application_id'] = application_id
                self.audit_dict['task_parent_id'] = steptask['name']
                self.audit_dict['task_exec_id']=self.task_exec_id
                self.audit_dict['task_cd']=self.audit_dict
                if steptask['recordCount'] is not None:
                    self.audit_dict['recordCount'] = steptask['recordCount']
                else:
                    self.audit_dict['recordCount']=0
                if '(' in parameter:
                    taskdecider_output,exec_id=self.writerauditing(my_dict,self.audit_dict, 'PU',
                                                                   operationalvariables,step_exec_id,
                                                                   pretask_df,pipelinename,argsdict,None,parameter,self.inputparam)
                else:
                    taskdecider_output,exec_id=self.writerauditing(my_dict,self.audit_dict, 'PU', operationalvariables,step_exec_id,
                                                                   pretask_df,argsdict,parameter,self.inputparam,pipelinename)
                self.audit_dict['task_exec_id'] = exec_id

                self.logobj.info(self.step_end_msg + self.step_exec_msg + str(step_exec_id))
                return taskdecider_output
        except Exception as e:
            exec_info = ''.join(traceback.format_exception_(etype=type(e), value=e,tb=e.__traceback__))
            self.audit_dict['message'] = exec_info
            self.readerauditing(my_dict, self.audit_dict, 'E', operationalvariables)
            self.logobj.error['Error Message : ' + str(exec_info)]
            raise


















