import cx_Oracle
from Common.Utilities._utils import _utils

class Auditwriter:
    def __init__(self):
        self.factobj=_utils()
        configreaderobj=self.factobj.getfactory('configreader')
        filepath=configreaderobj.configSectionMap('APPLICATION_USER')
        configvar=str(filepath['user'])
        self.oracleobj=self.factobj.getfactory('oracle',configvar)
        self.datetimeobj=self.factobj.getfactory('datetime')


    def updatedecider(self,task_exec_id,update_type,dict_task=None,audit_flag=None,retval=None):
        if audit_flag != 'O':
            if update_type=='S':
                return self.stepauditupdater(task_exec_id,dict_task,audit_flag,retval)
            elif update_type=='T':
                return self.taskauditupdater(task_exec_id, dict_task, audit_flag, retval)
            elif update_type=='W':
                return self.workflowauditupdater(task_exec_id, dict_task, audit_flag, retval)
        else:
            return None,None

    def decider(self,dict_task,audit_flag=None,argsdict=None):
            if audit_flag not in ('S','E','RC','PC','WC'):
                if 'step_cd' in dict_task:
                    return self.stepauditwriter(dict_task['step_cd'],argsdict)
                elif 'task_cd' in dict_task:
                    return self.taskauditwriter(dict_task['task_cd'])
                elif 'workflow' in dict_task:
                    return self.workflowauditwriter(dict_task['workflow'], argsdict)
            else:
                return None

    def stepauditupdater(self,work_exec_id,task_dict=None,audit_flag=None,retval=None):
            if audit_flag=='E':
                message = 'Workflow has been failed with exception ;'+task_dict['message'][:3900]
            else:
                message = 'workflow read successful'
            vendtime = self.datetimeobj.datetime.now()
            if work_exec_id is None:
                work_exec_id=task_dict['step_exec_id']
            sql_params = (work_exec_id,message,vendtime,vendtime,work_exec_id)
            sqlstrings="update octopus_step_execution set Status=(select status from octopus_task_execution where task_exec_id=" \
            "(select max(task_exec_id) from octopus_task_execution where step_exec_id=:1)),Message=:2,modifed_datetime =:," \
                  "end_time=:4,modified_by=user where step_exec_id=5"
            return work_exec_id,self.oracleobj.execute(sqlstrings,sql_params)

    def taskauditupdater(self,task_exec_id,task_dict,audit_flag,retval):
            if audit_flag=='RU':
                records_updated=task_dict['recordCount']
                vendtime = self.datetimeobj.datetime.now()
                if task_dict['sourceidentifier'] is None:
                    message = 'Reading from pretask '
                else:
                    message='Reader from source '+task_dict['sourceidentifier']
                sql_params = (message,records_updated,vendtime,vendtime,task_exec_id)
                sqlstrings="update octopus_task_execution set Status='S',message1,records_read:2,modifed_datetime=:3,end_time=:4,modified_by=user where task_exec_id=5"
            elif audit_flag=='WU':
                vendtime = self.datetimeobj.datetime.now()
                message='Written to Target'+task_dict['inputsource']
                sql_params = (message,task_exec_id,retval,vendtime,vendtime,task_exec_id)
                sqlstrings = "update octopus_task_execution set Status='S',message1,records_inserted=(select nvl(records_inserted,0) " \
                            "from octopus_task_execution where task_exec_id=:2 )+:3,modified_datetime =:4,end_time= :5,modified_by=user" \
                             " where task_exec_id=:6"
            elif audit_flag=='PU':
                vendtime = self.datetimeobj.datetime.now()
                status = retval['STATUS']
                message='Records has been processed by '+task_dict['task_name']+' process'
                records_updated = task_dict['recordCount']
                step_exec_id=task_dict['step_exec_id']
                vapplicationid= str(task_dict['application_id'])
                vtaskid = task_dict['task_name']
                sql_params = (status,message,records_updated,vendtime,vendtime,step_exec_id,vtaskid,vapplicationid)
                sqlstrings = "update octopus_task_execution set Status='0',message1,records_processed=:2,modified_datetime=:3" \
                              "end_time= :4,modified_by=user where step_exec_id=:5 and task_id(select task_id from octopus_task" \
                             " where task_cd=:6 and application_id7)"
            elif audit_flag == 'RC':
                step_exec_id = task_dict['step_exec_id']
                task_exec_id=task_dict['task_exec_id']
                records_updated = task_dict['recordCount']
                vendtime = self.datetimeobj.datetime.now()
                sql_params = (vendtime,vendtime,records_updated,step_exec_id,task_exec_id)
                sqlstrings = "update octopus_task_execution set Status='0',modified_datetime =:1,end_time= :2,records_read+;3," \
                             "modified_by=user where step_exec_id=:4 and task_id=:5"
            elif audit_flag=='WC':
                step_exec_id = task_dict['step_exec_id']
                task_exec_id = task_dict['task_exec_id']
                records_updated = task_dict['recordCount']
                vendtime = self.datetimeobj.datetime.now()
                sql_params = (vendtime,vendtime,records_updated,step_exec_id,task_exec_id)
                sqlstrings = "update octopus_task_execution set Status='0',modified_datetime =:1,end_time= :2,records_read+;3," \
                             "modified_by=user where step_exec_id=:4 and task_id=:5"
            elif audit_flag=='E':
                step_exec_id = task_dict['step_exec_id']
                application_id = task_dict['application_id']
                message='workflow has been failed with error '+task_dict['message'][:3900]
                vendtime = self.datetimeobj.datetime.now()
                vtaskid = task_dict['task_name']
                sql_params = (message, vendtime, vendtime, step_exec_id, vtaskid, application_id)
                sqlstrings = "update octopus_task_execution set Status='0',modified_datetime =:1,end_time= :2,records_read+;3," \
                             "modified_by=user where step_exec_id=:4 and task_id=(select task_id from octopus_task where task_cd=:5" \
                             " and application_id=:6) AND STATUS != 'S"

            else:
                vendtime = self.datetimeobj.datetime.now()
                sql_params = (vendtime, vendtime, task_exec_id)
                sqlstrings = "update octopus_task_execution set Status='0',modified_datetime =:1,end_time= :2,modified_by=user " \
                               "where task_exec_id=:3"
            return task_exec_id,self.oracleobj.execute(sqlstrings,sql_params)

    def workflowauditupdater(self,workflow_exec_id,task_dict=None,audit_flag=None,retval=None):
                if audit_flag == 'E':
                    message = 'Workflow has been failed with exception ;' + task_dict['message'][:3900]
                    status='E'
                else:
                    message = 'workflow read successful'
                    status='S'
                if workflow_exec_id is None:
                    workflow_exec_id = int(task_dict['workflow_exec_id'])
                    vendtime = self.datetimeobj.datetime.now()
                sql_params = (status, message, vendtime, vendtime, workflow_exec_id)
                return workflow_exec_id,self.oracleobj.execute("update octopus_workflow_execution set Status=:1,Message=:2,"
                                   "modified_datetime =:3,end_time=:4,modified_by=user where "
                                   "workflow_execution_id=:5",sql_params)

    def workflowauditwriter(self, dict_task,argsdict=None):
                workflow_exec_id = self.oracleobj._cursor.var(cx_Oracle.NUMBER)
                vworkflowid = dict_task['workflow_id']
                vapplicationid = dict_task['application_id']
                vstarttime = self.datetimeobj.datetime.now()
                vstatus = "R"
                vcreatedatetime = self.datetimeobj.datetime.now()
                sql_params = (
                vworkflowid, vstarttime, vstatus, vcreatedatetime,self.agrdictval(argsdict,'arg1'),self.agrdictval(argsdict,'arg2'),
                self.agrdictval(argsdict, 'arg3'), self.agrdictval(argsdict, 'arg4'),self.agrdictval(argsdict,'arg5'),
                self.agrdictval(argsdict,'arg6'),self.agrdictval(argsdict, 'arg7'), self.agrdictval(argsdict, 'arg8'),
                self.agrdictval(argsdict,'arg9'),self.agrdictval(argsdict,'arg10'),str(argsdict),vapplicationid, workflow_exec_id)
                self.oracleobj.execute('INSERT INTO octopus_workflow_execution(workflow_execution_id,workflow_id,START_TIME,STATUS '
                                       ',CREATED_BY,CREATED_DATETIME,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9,arg10,'
                                       'argdict,application_id) VALUES(octopus_sg_workflow_execution.NEXTVAL,:1,:2,:3,user,'
                                       ':4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16) RETURNING workflow_execution_id INTO :17', sql_params)
                return workflow_exec_id.getvalue()[0]




    def taskauditwriter(self,dict_task):
                new_task_exec_id=self.oracleobj._cursor.var(cx_Oracle.NUMBER)
                vtaskid=dict_task['task_name']
                vstepexecid=dict_task['step_exec_id']
                vtaskparentid=dict_task['task_parent_id']
                vapplicationid=dict_task['application_id']
                vstarttime = self.datetimeobj.datetime.now()
                vstatus = "R"
                vcreatedatetime = self.datetimeobj.datetime.now()
                sql_params =(vtaskid,vapplicationid,vstepexecid,vtaskparentid, vapplicationid,vstatus,vstarttime,vcreatedatetime,
                             vapplicationid,new_task_exec_id)
                self.oracleobj.execute('insert into octopus_step_execution(step_exec_id,step_id,workflow_execution_id,status,'
                                       'start_time,created_by,created_datetime,application_id) values(octopus_sq_step_execution,nextval),'
                                       '(select step_id from octopus_step where step_cd=:1 and application_id=:2),:3,:4,:5,user,:6,:7) '
                                       'returning step_exec_id into :8',sql_params)
                return new_task_exec_id.getvalue()

    def agrdictval(self,argsdict,vkey):
        if vkey in argsdict.keys():
            return argsdict[vkey]
        else:
            return None
                

























