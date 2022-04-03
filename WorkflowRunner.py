import datetime
import yaml
# import sys
# from configdetails import configdetails as cd
# filepath=cd.configsectionMap( 'OCTOPUS_ PATH ')
# octo_path = str (filepath( ' file_path'))
# sys.path.append(octo_path)
from Common.AuditProcess.Auditordecorator import Auditordecorator
from Core.TaskParser.workflowreader import _workflowreader
from Common.Utilities._utils import _utils
from Common.Logger._Metadata import  _Metadata
from Common.Logger.logger import logger
from Common.Utilities.AuditTableCreator import  AuditTableCreator
import traceback
import sys
import re
import time

now = datetime.datetime.now()
orderdate=now.date()
class WorkflowRunner:

    def __init__(self):
        self.auditdict1={}
        self.auditdict={}
        self.workflow_exec_id = None
        self.applicationid = None
        self.applicationname = None

    @Auditordecorator()
    def statusupdater(self,my_dict,auditdict,audit_flag=None,workflow_exec_id=None):
        return None
    @Auditordecorator()
    def workflowauditing(self,my_dict,auditdict,audit_flag=None,argsdict=None,workflow_exec_id=None):
        self.workflow_exec_id=workflow_exec_id
        #self.vf_nm = self.auditdict1['application_name']  + '_' + self.auditdict1['workflow_cd']
        #self.vf_nm = self.auditdict1['vorkflow_cd']
        self.log = logger(self.auditdict1['workflow_cd'], int(workflow_exec_id))
        self.logobj = self.log.getcustomlogger()
        self.logobj.info('starting the workflow name : ' + self.auditdict1['workflow_cd'])
        self.logobj.info('Aruguments Passed to workflow are %s' % argsdict)
        self.logobj.debug('Executing method : ' + __name__+ 'workflow parameter are:' + str(self.auditdict))
        self.logobj.debug('Executing method : ' +__name__+ 'workflow YAML is:' + str(my_dict))
        workflowreader = _workflowreader()
        return workflowreader.workflowread(my_dict,workflow_exec_id,argsdict, str(int(self.applicationid)),self.applicationname)

    def yamlreader(self, applicationname=None, workflowname=None,argsdict=None):
        try:
            metaauditentry = AuditTableCreator()
            factobj = _utils()
            #self.applicationname = applicationname
            #metaobj = _Metadata(aapplication+'_'+workflowname)
            configreaderobj = factobj.getfactory('configreader')
            filepath = configreaderobj.configSectionMap('APPLICATION_USER')
            path = str(configreaderobj.configSectionMap('CONFIG_PATH')['file_path'])
            pathseparater =str(configreaderobj.configSectionMap('DELIMITERFORPATH')['delimiter'])
            inilist = [m.start() for m in re.finditer(pathseparater,workflowname)]
            if inilist:
                loc = inilist[-1] + 1
                wf_nm = workflowname[loc:]
            else:
                wf_nm = workflowname
            name = str(path) + workflowname + pathseparater + wf_nm + '.yml'
            self.applicationname = applicationname
            workflow_yaml=yaml.load(open(name))
            self.applicationid =metaauditentry.metaapplication(applicationname)
            metaauditentry.metaworkflow(workflow_yaml, self.applicationid)
            configvar = str(filepath['user'])
            oracleobj = factobj.getfactory('oracle', configvar)
            param = wf_nm
            sql = "select workflow_id,workflow_cd,application_id,workflow_name from octopus_workflow where workflow_cd ='"+str(param)\
                  +"'and application_id ='" +  str(self.applicationid) + "'"
            print(sql)
            df = oracleobj.dbselectall(sql)
            #self.log = logger(applicationname + '_' + workflowname)
            #self.logobj = self.log.getcustomlogger()
            #self.logobj.info( 'starting the workflow name : ' + workflowname)
            self.auditdict1['workflow_id'] = int(df['WORKFLOW_ID'].values[0])
            self.auditdict1['workflow_cd'] = str(df['WORKFLOW_CD'].values[0])
            self.auditdict1['application_id'] = str(df['APPLICATION_ID'].values[0])
            #self.auditdict1['application_name'] = str(self.applicationname)
            #self.auditdict1['workflowname'] = str(workflowname),
            self.auditdict['workflow']=self.auditdict1
            #self.logobj.debug('Executing method : '+__'Workflow parameter are' + str(self.auditdict))'
            #self.logobj.debug('Executing method : ''__Workflow YAML is' + str(workflow_yaml))
            self.workflowauditing(workflow_yaml, self.auditdict,None,argsdict)
            #self.logobj.info('workflow Ended : 'df['WORKFLOW_CD'].values[0])
        except Exception as e:
            exc_info = ' '.join(traceback.format_exception(etype=(e), value=e, tb=e.__traceback__))
            self.auditdict['message']=exc_info
            self.auditdict['workflow_exec_id']=self.workflow_exec_id
            self.statusupdater(workflow_yaml, self.auditdict,'E')
            self.logobj.error(' Error Message : ' + str(exc_info))
            exit(1)

if __name__ == '__main__':
      start_time = time.time()
      argdict={}
      pipelinename = sys.argv[1]
      applicationname = sys.argv[2]
      for i in range(3,sys.argv.__len__()):
          argdict['arg' + str(i -2)] = sys.argv[i]
      readerobj = WorkflowRunner()
      outputval = readerobj.yamlreader(applicationname, pipelinename, argdict)
      end_time= time.time()
      print(f"\nworkflow Excution has been completed in (end_time - start_time) seconds !!")

