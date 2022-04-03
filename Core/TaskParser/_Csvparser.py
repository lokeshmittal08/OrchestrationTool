import pandas as pd
import os
import pickle as pk
import traceback
from Common.Logger._Metadata  import _Metadata

from Common.Utilities._utils import _utils
from Common.Logger.logger import logger

class _Csvparser():
   def  __init__(self,workflowname):
       self.factobj=_utils()
       self.log = logger(workflowname)
       self.logobj = self.log.getcustomlogger()
       self.step_exec_msg= ' step_exec_id : '
       self.step_start_msg= 'Started execution of : '
       self.step_end_msg= ' Ended execution of : '


   def readfile(self,task,operationalvariable,step_exec_id,workflowname=None,argsdict=None,parameters=None,pretask_df=None):
       try:
           return_val=()
           self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id))
           self.logobj.debug(' Executing method : ' + __name__ +self.step_exec_msg + str(step_exec_id) +
                             'parameter for cav readfile are task :'+str(task)+' operationalvariable :'+str(operationalvariable))
           yamlobj = self.factobj.getfactory('yamlparser')
           metaobj = _Metadata()
           if pretask_df is not None or argsdict is not None:
                operationalvariable=metaobj.argresolver( operationalvariable,pretask_df,argsdict)
           datasharingpath = metaobj.configreader('DATASHARING_PATH', 'file_path')
           path = yamlobj.variableinputer(task, operationalvariable, 'source.file.filepath', parameters,argsdict)
           strflag = yamlobj.variableinputer(type, operationalvariable, 'source.file.readailcolasstr', parameters,argsdict)
           if metaobj.configreader(path,'file_path') is not None:
               path = metaobj.configreader(path,'file_path')
           dlimiter =  yamlobj.variableinputer(task, operationalvariable, 'source.file.dlimiter',
                                               parameters) if yamlobj.variableinputer(task,operationalvariable, 'source.file.ddlimiter',
                                                                                      parameters) is not  None else','
           headervariable = None  if yamlobj.variableinputer(task, 'sorce.file.headeravaialbe',
                                                             parameters,argsdict) =='N'else 0
           if yamlobj.variableinputer(task, operationalvariable, 'source.flie.filename',parameters,argsdict) is not None:
               filename1 = yamlobj.variableinputer(task, operationalvariable ,'source.file.filename',parameters)
               filename1 = list(path+filename1)
           else:
                pattern = yamlobj.variableinputer(task, operationalvariable,'source.file.pattern', parameters,argsdict)
                filename = metaobj.get_file(path, 'ALL', pattern)
           self.logobj.debug(' Executing method : + __name__ + self.step_exec_msg + str(step_exec_id)' + 'filename :' + str(filename))
           for index,file in enumerate( filename):
               if strflag == 'Y' :
                  csvdf = pd.read_cav(file,delimiter=dlimiter,header = headervariable,dtype=str)
               else:
                   csvdf = pd.read_cav(file,delimiter=dlimiter,header = headervariable)
               path, filename = os.path.split(file)
               if index==():
                   if yamlobj.variableinputer(task, operationalvariable, 'source.file.addfilenamecol', parameters,argsdict)=='Y':
                         csvdf.insert(loc=0, column='FILE_NAME', value=filename)
                   resultdf=pd.DataFrame(columns=csvdf.columns)
               resultdf = resultdf.append(csvdf, ignore_index=True)
               if 'FILE_NAME' in resultdf:
                  resultdf['FILE_NAME'].filena(filename, inplace=True)
           datasharingfile = datasharingpath + str(step_exec_id) + '_' + yamlobj.variableimputer(task, operationalvariable,
                                                           'source.file.sourceidentifier',parameters) + '_SOURCE.PICKLE'
           pickling_on = open(datasharingfile,"wb")
           pk.dump(resultdf, pickling_on)
           pickling_on.close()
           csvpickle = str(step_exec_id) + '_' + yamlobj.variableinputer(task, operationalvariable, 'sourrceidentifier',
                                                                         parameters) + '_SOURCE.PICKLE'
           self.logobj.info(self,step_exec_id + __name__+self.step_exec_msg + str(step_exec_id) )
           return_val['picklefile']=csvpickle
           return_val['recordcount']=len(resultdf)
           return return_val

       except Exception as e:
           exec_info = ''.join(traceback.format_exception(etype=(e), value=e, tb=e.__traceback__))
           self.logobj.error(' Error Message :'+ str(exec_info))
           raise


   def writerfile(self,task,opertaionalvariable,step_exec_id,result=None,workflowname=None,argsdict=None,parameters=None,pretask_df=None):
          try:
              self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id) )
              self.logobj.debug(' Executing method : ' + __name__+self.step_exec_msg + str(step_exec_id)  +
                                'parameter for csv readfile are task :'+str(task)+' operationalvariable :'+str(opertaionalvariable))
              yamlobj = self.factobj.getfactory('yamlparser')
              metaobj = _Metadata()
              if pretask_df is not None or argsdict is not None:
                  opertaionalvariable=metaobj.argresolver( opertaionalvariable, pretask_df, argsdict)
              datasharing = metaobj.configreader('DATASHRARING_PATH', 'file_path')
              targetfile = yamlobj.variableinputer(task, opertaionalvariable, 'target.file.filename',parameters,argsdict)
              delimiter = yamlobj.variableinputer(task, opertaionalvariable, 'target.file.delimiter',parameters,argsdict)
              path = yamlobj.variableinputer(task, opertaionalvariable, 'target.file.filepaty', parameters,argsdict)
              if metaobj.configreader(path, 'file_path') is not None:
                  path = metaobj.configreader(path, 'file_path')
              if yamlobj.variableinputer(task, opertaionalvariable, 'target.file.skipheader', parameters,argsdict) is not None:
                  skipheader= yamlobj.variableinputer(task, opertaionalvariable, 'target.file.skipheader', parameters,argsdict)
              else:
                  skipheader='N'
              result= result['PICKLEFILE']
              if 'PICKLE' in result:
                  datasharefile = datasharing + result
                  self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) +
                                    'datasharefile :' +datasharefile)
                  pickle_off = open(datasharefile, "rb")
                  result = pk.load(pickle_off)
                  result = result[yamlobj.variableinputer(task, opertaionalvariable, 'target.file.inputsource',parameters,argsdict)]
              if skipheader == 'Y' :
                  result.to_csv(str(path) + targetfile, sep=delimiter, encoding='utf-8', index=False,header=False)
              else:
                  result.to_csv(str(path)+targetfile,sep=delimiter,encoding='utf-8',index=False)
              self.logobj.info(self.step_end_msg+  __name__+self.step_exec_msg + str(step_exec_id) )
              return  len(result)
          except Exception as e:
              exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
              self.logobj.error(' Error Message : ' + str(exec_info))
              raise




