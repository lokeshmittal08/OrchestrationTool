from functools import wraps
from Common.AuditProcess.Auditinginterface import AuditingInterface



class Auditordecorator(object):
    def __init__(self):
        self.auditobj=AuditingInterface()


    def __call__(self, orginal_func):
        @wraps(orginal_func)
        def wrapped_f(*args):
            if 'workflow' in args[2] and args[3] not in ('S','E','RC','PC','WC'):
                workflow_exec_id = self.auditobj.auditwriterable(args[2],args[3],args[4])
            else:
                workflow_exec_id = self.auditobj.auditwriterable(args[2], args[3])
            args=(*args,workflow_exec_id)
            retval=orginal_func(*args)
            if 'step_cd' in args[2]:
                exec_id,no_of_record_updated = self.auditobj.auditupdatable(workflow_exec_id,'S',args[2],args[3],retval)
            elif 'task_cd' in args[2]:
                exec_id, no_of_record_updated = self.auditobj.auditupdatable(workflow_exec_id, 'T', args[2], args[3],
                                                                               retval)
            elif 'workflow' in args[2]:
                exec_id, no_of_record_updated = self.auditobj.auditupdatable(workflow_exec_id, 'W', args[2], args[3],
                                                                               retval)
            if workflow_exec_id is None:
                workflow_exec_id=exec_id
            return retval,workflow_exec_id
        return wrapped_f





