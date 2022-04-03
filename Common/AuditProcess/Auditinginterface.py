from Common.AuditProcess.Auditwriter import Auditwriter

class AuditingInterface:

    def __init__(self):
        self.auditobj=Auditwriter()

    def auditwriterable(self,dict_task,audit_flag=None,argsdict=None):
        return  self.auditobj.decider(dict_task,audit_flag,argsdict)

    def auditupdatable(self,task_exec_id,update_type,dict_task=None,audit_flag=None,retval=None):
        return self.auditobj.updatedecider(task_exec_id,update_type,dict_task,audit_flag,retval)

