import yaml
from Common.AuditProcess.Auditordecorator import Auditordecorator
from Core.TaskController._controller import  _controller
from Common.Utilities._utils import _utils
from Common.Logger._Metadata import _Metadata
from Common.Logger.logger import logger
import traceback
from Common.Utilities.AuditTableCreator import AuditTableCreator


class _workflowreader:
    def __init__(self):

        self.inputparam = None
        self.audit_dict = {}
        self.processfile = ''
        self._workflow_exec_msg = ' pipeline_exec_id :'
        self._workflow_start_msg = ' Started execution of :'
        self._workflow_end_msg = ' Ended execution of : '
        self.workflow_exec_id = None
        self.metaobj = _Metadata()
        self.metaauditentry = AuditTableCreator
        self.factobj = _utils()
        self.counter = ()

    @Auditordecorator()
    def statusupdater(self, my_dict, auditdict, audit_flag=None, workflowname=None, workflow_exec_id=None):
        return None

    @Auditordecorator()
    def stepauditing(self, my_dict, auditdict, audit_flag=None, workflowname=None, argsdict=None, step_exec_id=None):
        self.step_exec_id = step_exec_id
        controller = _controller(workflowname)
        application_id = auditdict['application_id']
        application_name = auditdict['application_name']
        return controller.parse(my_dict,step_exec_id, workflowname, argsdict, application_id, application_name)

    def metadatacreator(self, task, tasktype, application_id):
        if self.metaobj.taskvalue(task,tasktype) is not None:
            for iter in self.metaobj.gettaskvalue(task, tasktype):
                if '('in iter['name']:
                    taskname = iter['name'].spilt('(')[0]
                else:
                    taskname = iter['name']
                self.metaauditentry.metatask(taskname, application_id)

    def workflowread(self, workflowtask, workflow_exec_id=None,argsdict=None, application_id=None,
                     application_name=None):
        try:

            path = self.metaobj.configreader('CONFIG_PATH', 'file_path')
            pathseparater = self.metaobj.configreader('DELIMITERFORPATH', 'delimiter')
            datasharingpath = self.metaobj.configreader('DATASHARING_PATH', 'file_path')
            utilobj = _utils()
            yamlobj = utilobj.getfactory('yamlparser')
            workflowname = yamlobj.getvalue(workflowtask, 'name')
            stepfailurerecovery = yamlobj.getvalue(workflowtask, 'stepfailurerecovery')
            self.log = logger(workflowname)
            self.logobj = self.log.getcustomlogger()
            self.audit_dict['workflow_exec_id'] = workflow_exec_id
            self.audit_dict['application_Id'] = application_id
            print('Inside the oracledataread')
            self.oracleobj = self.factobj.getfactory('oracle', 'DB_CON')

            sql = 'select WORKFLOW_EXECUTION_ID, status from c##scott.octopus_workflow_execution  Where workflow_execution_id= '\
                ' (SELECT WORKFLOW_EXECUTION_ID FROM (select WORKFLOW_EXECUTION_ID , DENSE_RANK() OVER '\
                ' (PARTITION BY WORKFLOW_ID ORDER BY WORKFLOW_EXECUTION_ID DESC ) AS RNK '\
                ' from c##scott.octopus_workflow_execution where WORKFLOW_ID in'\
                '(select WORKFLOW_ID from OCTOPUS_WORKFLOW WHERE WORKFLOW_name= '+ "'" + workflowname\
                 + "' and application_id = " +str(
                application_id) + " ) ) WHERE RNK=2 )"
            print(sql)
            workflowstat = self.oracleobj.dbselectall(sql)
            if stepfailurerecovery != 'N' :
                 if workflowstat['STATUS'].values == 'E':
                     sql = "SELECT STEP_NAME FROM (SELECT S.STEP_NAME , DENSE_RANK() OVER(PARTITION BY SE.STEP_ID ORDER BY STEP_EXEC_ID " \
                           "DESC ) AS RNK FROM c##scott.OCTOPUS_STEP_EXECUTION SE , c##scott.OCTOPUS_STEP S WHERE SE.STEP_ID=S.STEP_ID "\
                           "AND SE.STATUS= 'E' AND SE.WORKFLOW_EXECUTION_ID = (select WORKFLOW_EXECUTION_ID from " \
                           "c##scott.octopus_workflow_execution where workflow_execution_id= (SELECT WORKFLOW_EXECUTION_ID FROM" \
                           " (select WORKFLOW_EXECUTION_ID , DENSE_RANK() OVER (PARTITION BY WORKFLOW_ID " \
                           "ORDER BY WORKFLOW_EXECUTION_ID DESC ) AS RNK from c##scott.octopus_workflow_execution  " \
                            "where WORKFLOW_ID in (select WORKFLOW_ID from c##scott.OCTOPUS_WORKFLOW WHERE WORKFLOW_name=" + "'"  +\
                            workflowname + "' and application_id = '" + str(
                            application_id) + "') ) WHERE RNK=2 )AND      STATUS= 'E')) WHERE RNK=1"
                     print(sql)
                     failedstepname = self.oracleobj.dbselectall(sql)
                     for i in range(workflowtask['steps'].__len__()):
                         steptype = yamlobj.getvalue(workflowtask[ 'steps'](i), 'step.type')
                         stepname = yamlobj.getvalue(workflowtask[ 'steps'](i), 'step.type')
                         if failedstepname.empty:
                             self.counter = 0
                         elif (failedstepname.values[0] == stepname):
                             self.counter = i
            for i in range(self.counter, workflowtask['steps'].__len__()):
                steptype = yamlobj.getvalue(workflowtask['steps'](i), 'step.type')
                stepname = yamlobj.getvalue(workflowtask['steps'](i),'step.name')
                dep_step = yamlobj.getvalue(workflowtask['steps'](i), 'step.dep_step')
                dep_step_status = yamlobj.getvalue(workflowtask['steps'](i), 'steps.dep_step_status')
                if dep_step is not None and dep_step_status is not None:
                    self.oracleobj._cursor.execute(
                       "select step_id from c##scott.octopus_step where stepname=:1 and application_id =:2",
                        [dep_step, str(application_id)])
                    dep_step_id = self.oracleobj._curser.fetchone()[0]
                    self.oracleobj._curser.execute(
                       "select status from c##scott.octopus_step_execution where workflow_execution_id=:1 and application_id =:2 and step_id= :3",
                       [workflow_exec_id, str(application_id), dep_step_id])
                    step_dep_status = str(self.oracleobj._curser.fetchone()[0]).strip()
                    if dep_step_status != step_dep_status:
                       continue
                self.logobj.info(self._workflow_start_msg + __name__ +self.workflow_exec_msg + str(workflow_exec_id))
                taskname = path + workflowname + pathseparater + stepname + '.yml'
                my_dict = yaml.load(open(taskname))
                stepname = yamlobj.getvalue(my_dict, 'stepName')
                self.metaauditentry.metastep(my_dict,str(application_id))

                self.metadatacreator(my_dict, 'Pretask', str(application_id))
                self.metadatacreator(my_dict, 'Reader', str(application_id))
                self.metadatacreator(my_dict, 'Processer', str(application_id))
                self.metadatacreator(my_dict, 'Writer', str(application_id))
                self.metadatacreator(my_dict, 'Posttask',str(application_id))

                self.logobj.debug('Executing meathod : ' + __name__ + ' workflow YAML: ' + str(my_dict))
                self.logobj.info('workflow YAML is : ' + str(my_dict))
                self.audit_dict['step_code'] = stepname
                self.audit_dict['step_cd'] = self.audit_dict
                self.logobj.info(' Executing method ; ' + __name__ + self.workflow_exec_msg + str(
                    workflow_exec_id) + ' parameter for workflow are ' + str(self.audit_dict))
                self.audit_dict['application_id'] = application_id
                self.audit_dict['application_name'] = application_name
                self.audit_dict['workflow_exec_id'] = workflow_exec_id
                self.stepauditing(my_dict, self.audit_dict, None, workflowname,argsdict)
                filecleanup = datasharingpath + '+' + str(self.step_exec_id) + '*'
                self.logobj.info(self._workflow_end_msg + __name__ + self.workflow_exec_msg + str(workflow_exec_id))
                self.metaobj.fileremoval(filecleanup)

        except Exception as e :
                exc_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                self.audit_dict['message'] = exc_info
                self.audit_dict['step_exec_id'] = self.step_exec_id
                self.statusupdater(my_dict, self.audit_dict, 'E', workflowname)
                self.logobj.error(' Error Message : ' + str(exc_info))
                self.metaobj.fileremoval(datasharingpath + pathseparater + str(self.workflow_exec_id) + '*')
                raise




