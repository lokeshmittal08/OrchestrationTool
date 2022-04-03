import pandas as pd
import pickle as pk
import traceback

from Common.Logger._Metadata import _Metadata

from Common.Utilities._utils import _utils

from Common.Logger.logger import logger


class _Excelparser():
    def __init__(self,workflowname):
        self.factobj=_utils()
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg=' step_exec_id :'
        self.step_start_msg=' Started execution of :'
        self.step_end_msg = 'Ended execution of :'


    def readfile(self,task,operationvariable,step_exec_id,workflow=None,argdict=None,paramerts=None,pretask_df=None):
        try:
            return_val = ()
            self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id))
            self.logobj.debug(' Executing method : '+ __name__+self.step_exec_msg+ str(step_exec_id) +
                              'parameter for excel readfile are task :'+str(task)+' operationvariable :'+str(operationvariable))
            print(' Executing method : '+__name__+self.step_exec_msg + str(step_exec_id) +
                  'parameter for excel readfile are task :'+str(task)+' operationvariable :'+str(operationvariable))
            yamlobj = self.factobj.getfactory('yamlparser')
            metaobj = _Metadata()
            if pretask_df is not  None or argdict is not None:
                operationlvariable=metaobj.argsolver( operationvariable, pretask_df, argdict)
            datasharingpath = metaobj.configreader('DATASHARING_PATH', 'file_path')
            filepath = yamlobj.variableinputer(task, operationvariable, 'source.file.filepath',paramerts)
            if yamlobj.variableinputer(task, operationvariable, 'source.file.filename',paramerts) is not None:
                filename1= yamlobj.variableinputer(task, operationvariable, 'source.file.filename',paramerts)
                filename1=filepath+filename1
            else:
                pattern=yamlobj.variableinputer(task, operationvariable, 'source.file.pattern', paramerts)
                filelist=metaobj.get_file(filepath, 'ASCREAD' , pattern)
                srcfile=filelist[0]
                filename = srcfile
            print(filename)
            self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) + 'filename :' + str(filename))
            file=pd.ExcelFile(filename)
            sheets =file.sheet_names
            exceldf=pd.DataFrame()
            exceldf1 = pd.DataFrame()
            sheet_to_df_map = ()
            for sheet in file.sheet_names:
                sheet_to_df_map[sheet] = file.parse(sheet)
            print(sheet_to_df_map[sheet])
            datasharingpathfile = datasharingpath + str(step_exec_id) + '_' + yamlobj.variableinputer(task,operationvariable,
                                                                  'source.file.sourceidentifier',paramerts) + '_SOURCE.PICKLE'
            pickling_on = open(datasharingpathfile, "wb")
            recordcount = len(sheet_to_df_map[sheet])
            print(recordcount)
            pk.dump(sheet_to_df_map, pickling_on)
            pickling_on.close()
            xlsdf = str(step_exec_id) + '_' + yamlobj.variableinputer(task, operationvariable,
                                                                      'source.file.sourceidentifier',paramerts) + '_SOURCE.PICKLE'
            datasharingpathfile = xlsdf
            return_val['picklefie'] = datasharingpathfile
            return_val['recordcount'] = recordcount
            return_val['STSTUS'] ='S'
            self.logobj.info(self.step_end_msg + __name__+self.step_exec_msg + str(step_exec_id) )
            print('completed')
            return  return_val
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e),value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message :'  + str(exec_info))
            raise


    def writerfile(self,task,operationvariable,step_exec_id,result=None,workflowname=None,argdict=None,parameters=None ,pretask_df=None):
        try:
            self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id) )
            self.logobj.debug(' Executing method : ' + __name__+self.step_exec_msg + str(step_exec_id) +
        'parameter for excel readfile are task :'+str(task)+' operationvariable :'+str(operationvariable)+
                              ' command line argument :'+str(argdict))
            yamlobj = self.factobj.getfactory('yamlparser')
            metaobj = _Metadata()
            if pretask_df is not None or argdict is not None:
                operationvariable=metaobj.argsolver( operationvariable, pretask_df, argdict)
            datasharingpath = metaobj.configreader(' DATASHARING_PATH', 'file_path')
            targetfilepath = yamlobj.variableinputer(task, operationvariable, 'target.file.filepath', parameters)
            targetfile = yamlobj.variableinputer(task, operationvariable, 'target.file.filename',parameters)
            sheetname = yamlobj.variableinputer(task, operationvariable, 'target.file.sheetname',parameters)
            headerreguired = yamlobj.variableinputer(task, operationvariable, 'target.file.headerreguired',parameters)
            if headerreguired == 'N' or headerreguired =='n' :
                headerreg=False
            else:
                headerreg=True

            if 'PICKLE' in result:
                datasharingfile = datasharingpath + result
                self.logobj.debug(' Executing method : '+ __name__ + self.step_exec_msg + str(step_exec_id) + 'datasharefile:'
                                 + datasharingfile)
                pickle_off = open(datasharingfile, "rb")
                result = pk.load(pickle_off)
                result = result[yamlobj.variableinputer(task, operationvariable, 'target.file.inputsource',parameters)]

            if targetfilepath is not None:
                writer = pd.ExcelWriter(str(targetfilepath)+targetfile)
            else:
                writer = pd.ExcelWriter(targetfile)

            for sheet,frame in result.items():
                frame.to_excel(writer, sheet_name=sheet, encoding='utf-8', index=False)
            writer.save()

            self.logobj.info(self.step_end_msg + __name__+self.step_exec_msg + str(step_exec_id) )
            return 5
        except Exception as e:
            exc_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error('Error Message : ' + str(exc_info))
            raise



