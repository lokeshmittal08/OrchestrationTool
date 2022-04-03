from __future__ import unicode_literals

import pickle as pk

import pandas as pd
import cx_Oracle
import traceback

from Common.Utilities._utils import _utils

from Common.Logger._Metadata import _Metadata

from  Common.Logger.logger import logger

import unicodedata
import os

pd.set_option('display.max_colwidth', -1)


class _Oracleparser:
    def __init__(self, workflowname):
        self.factobj = _utils()
        self.databasesql = None
        self.returndf = None
        self.chunksize = None
        self.querystring =''
        self.cool_names = ''
        self.objectname = ''
        self.metaobj = _Metadata(workflowname)
        self.yamlobj = self.factobj.getfactory('yamlparser')
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg= ' step_exec_id : '
        self.step_start_msg= 'Started execution of : '
        self.step_end_msg= ' Ended execution of : '

    def pretaskimputed(self, sqlstring, pretask_df):
        try:
            sqltolist = str(sqlstring).split('$$')
            for i in range(len(sqltolist)):
                print(pretask_df[pretask_df['SQLKEY'] == sqltolist[i]])
                print(pretask_df[pretask_df['SQLKEY'] == sqltolist[i]]['SQLVALUE'])
                print(len(pretask_df[pretask_df['SQLKEY'] == sqltolist[i]]['SQLVALUE']))
                if len(pretask_df[pretask_df['SQLKEY'] == sqltolist[i]]['SQLVALUE']) > 0:

                    str1 = (pretask_df[pretask_df['SQLKEY'] == sqltolist[i]]['SQLVALUE']).values[0]
                else:
                    str1 = sqltolist[1]
                self.querystring = self.querystring + str(str1)
            sqlstring = self.querystring
            return sqlstring
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise

    def argumentimputed(self, sqlstring, argdict):
            try:
                argquerystring = ''
                sqltolist = str(sqlstring).split('$$')
                for i in range(len(sqltolist)):
                    if sqltolist[i] in argdict.keys():
                        str1 = argdict[sqltolist[i]]
                    else:
                        str1 = sqltolist[i]
                    argquerystring = argquerystring + str(str1)
                return argquerystring
            except Exception as e:
                exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                self.logobj.error(' Error Message : ' + str(exec_info))
                raise

    def selectall(self, oracleobj, sqlstring, pre_task_df=None, argdict=None):
            try:
                if pre_task_df is not None:
                    retdict = self.metaobj.pickletodict(pre_task_df)
                    pretask_df = retdict
                    sqlstring = self.pretaskimputed(sqlstring, pretask_df)
                if argdict is not None:
                    sqlstring = self.argumentimputed(sqlstring, argdict)
                df = oracleobj.dbselectall(sqlstring)
                return df
            except Exception as e:
                exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                self.logobj.error(' Error Message : ' + str(exec_info))
                raise

    def fileremoval(self):
                os.removal()

    def chunkfetch(self, oracleobj, sqlstring, chunksize, step_exec_id, pretask_df=None, argdict=None):
                try:
                    result_df = None
                    if pretask_df is not None:
                        retdict = self.metaobj.pickletodict(pretask_df)
                        pretask_df = retdict
                        sqlstring = self.pretaskimputed(sqlstring, pretask_df)
                    if argdict is not None:
                        sqlstring = self.argumentimputed(sqlstring, argdict)
                    print(str(sqlstring))
                    oracleobj._cursor.execute(str(sqlstring))
                    appendflag = 'N'
                    rowcount = 0
                    for result in oracleobj.ResultIter(int(chunksize)):
                        if appendflag == 'N':
                           result_df = result
                           self.logobj.debug(' Executing method :chunkfetch' + self.step_exec_msg + str(
                                step_exec_id) + ' append flag is :' + appendflag + ' no of records fetched:' + str(
                                len(result_df)))
                           rowcount = rowcount + len(result_df)
                           appendflag = 'Y'
                        else:
                            result_df = result_df.append(result, ignore_index=True)
                            rowcount = rowcount + len(result_df)
                            self.logobj.debug(' Executing method :chunkfetch' + self.step_exec_msg + str(
                                  step_exec_id) + ' append flag is :' + appendflag + ' no of records fetched:' + str(
                                 rowcount))
                    if result_df is None:
                        result_df = oracleobj.dbselectall(sqlstring)
                    return result_df
                except Exception as e:
                    exc_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                    self.logobj.error(' Error Message : ' + str(exc_info))
                    raise

    def fetchdata(self,task, operationvariable, step_exec_id, pretask_df=None, workflowname=None, argdict=None,
                              parameters=None):
                try:
                    return_val = ()
                    self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                    self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                        step_exec_id) + 'parameter for db fetchdata are task :' + str(task) + ' operationvariable ;' + str(
                        operationvariable))
                    metaobj = _Metadata(workflowname)
                    yamlobj = self.factobj.getfactory('yamlparser')
                    sourceidentifier = yamlobj.variableimputer(task, operationvariable, 'source.db.sourceidentifier',
                                                               parameters)
                    if sourceidentifier is None:
                        sourceidentifier = 'S1'
                    path = metaobj.configreader('DATASHARING_PATH', 'file_path')
                    connection = yamlobj.variableimputer(task, operationvariable, 'source.db.name', parameters)
                    oracleobj = self.factobj.getfactory('oracle', connection)
                    if yamlobj.variableimputer(task, operationvariable, 'source.db.loadtype.sql.script',
                                               parameters) is not None:
                       self.databasesql = yamlobj.variableimputer(task,operationvariable, 'source.db.loadtype.sql.script',
                                               parameters)
                    else:
                        objectname = yamlobj.variableimputer(task, operationvariable, 'source.db.objectname',parameters)
                        collist = yamlobj.variableimputer(task, operationvariable, 'source.db.objectcolumnlist',parameters)
                        self.databasesql = metaobj.selectstatementpreparator(objectname, collist)
                    self.chunksize = yamlobj.variableimputer(task, operationvariable, 'source.db.chunksize',parameters)
                    if self.chunksize is not None:
                        self.logobj.debug(' Executing method : '+ __name__ + self.step_exec_msg + str(
                            step_exec_id) + 'Input parameter for chunkfetch are oracleobj : ' + str(
                            oracleobj) + ' databasesql :' + str(self.databasesql) + ' chunksize : ' + str(
                            self.chunksize) + 'pretask_df : ' + str(pretask_df))
                        self.returndf = self.chunkfetch(oracleobj, self.databasesql, self.chunksize, step_exec_id, pretask_df,
                                                        argdict)
                    elif yamlobj.variableimputer(task, operationvariable, 'source.db.fetchtype',parameters) == 'C':
                        self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(
                            step_exec_id) + ' fetchtype is compelete ')
                        self.logobj.debug(' Executing method :' + __name__ + self.step_exec_msg + str(
                             step_exec_id) + ' Input parameter for selectall are oracleobj : ' + str(
                            oracleobj) + ' databasesql :' + str(self.databasesql))
                        self.returndf = self.selectall(oracleobj, self.databasesql, pretask_df, argdict)
                    else:
                        self.returndf = self.chunkfetch(oracleobj, self.databasesql, 5000, step_exec_id, pretask_df, argdict)

                    recordcount = len(self.returndf)
                    if self.return_df is not None:
                        recordcount = 0
                        self.returndf = u"No Data fetched"

                    self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                    picklefilename = str(step_exec_id) + '_' + sourceidentifier + '_SOURCE.PICKLE'
                    datasharingfile = path + picklefilename

                    pickling_on = open(datasharingfile, "wb")
                    pk.dump(self.returndf, pickling_on)
                    pickling_on.close()
                    self.returndf = picklefilename
                    return_val['picklefile'] = self.returndf
                    return_val['recordcount'] = recordcount
                    return_val['STATUS'] = 'S'
                    return return_val
                except Exception as e:
                     exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                     self.logobj.error(' Error Message : ' + str(exec_info))
                     raise

    def loaddata(self, task,operationvariable, step_exec_id, result, workflowname, argdict=None,parameters=None,pretask_df=None):
                try:
                    self.logobj.info(self.step_exec_msg + str(step_exec_id) + self.step_start_msg + __name__)
                    self.logobj.debug(' Executing method : ' + __name__ +self.step_exec_msg + str(step_exec_id)  +
                        'parameter for db loaddata are task :'+str(task)+' operationvariable :'+str(
                        operationvariable))
                    yamlobj = self.factobj.getfactory('yamlparser')
                    metaobj = _Metadata()
                    if pretask_df is not None or argdict is not None:
                        operationvariable=metaobj.argresolver( operationvariable, pretask_df, argdict)
                    inputsource = yamlobj.variableimputer(task, operationvariable, 'target.db.inputsource', parameters)
                    if inputsource is not None:
                        inputsource = 'T1'
                    path = metaobj.configreader('DATASHARING_PATH', 'file_path')
                    connection = yamlobj.variableimputer(task,operationvariable, 'target.db.name', parameters)
                    oracleobj = self.factobj.getfactory('oracle', connection)
                    result = result['PICKLEFILE']
                    if 'PICKLE' in result:
                        datasharefile = path + result
                        pickle_off = open(datasharefile, "rb")
                        result = pk.load(pickle_off)
                        result = result[inputsource]
                    sqlscript = yamlobj.variableimputer(task, operationvariable, 'target.db.loadtype.sql.script', parameters)
                    if sqlscript is not None:
                        insertsql = sqlscript
                        self.col_names = str(insertsql).split('(')[1].split(')')[0]
                        self.objectname = str(insertsql).split('INFO')[1].split('(')[0]
                    else:
                        self.objectname = yamlobj.variableimputer(task, operationvariable, 'target.db.objectname', parameters)
                        targetcolumnlist = yamlobj.variableimputer(task, operationvariable, 'target.db.columnlist', parameters)
                        upsertkey = yamlobj.variableimputer(task, operationvariable, 'target.db.upsertkey', parameters)
                        deletekey = yamlobj.variableimputer(task, operationvariable, 'target.db.deletekey', parameters)
                        if upsertkey is not None:
                            auditinsertcols = yamlobj.variableimputer(task, operationvariable, 'target.db.auditinsertcols',
                                                                      parameters)
                            auditupdatecols = yamlobj.variableimputer(task, operationvariable, 'target.db.auditupdatecols',
                                                                      parameters)
                            meresql, insert_cols = metaobj.mergestatementpreparator(self.objectname, result.columns,
                                                                                    targetcolumnlist, upsertkey,
                                                                                    auditinsertcols, auditupdatecols)
                        elif deletekey is not None:
                            deletesql, delete_cols = metaobj.deletestatementprepartor(self.objectname, deletekey)
                            insertsql, insert_cols = metaobj.insertstatementprepartor(self.objectname, result.columns,
                                                                                      targetcolumnlist)
                        else:
                            insertsql, insert_cols = metaobj.insertstatementprepartor(self.objectname, result.columns,
                                                                                     targetcolumnlist)
                    dataretention = yamlobj.variableimputer(task, operationvariable, 'target.db.dataretention', parameters)
                    if dataretention is not None:
                        self.logobj.debug(' Execution method : ' + __name__ + self.step_exec_msg + str(
                             step_exec_id) + ' datatention :' + str(dataretention))
                        if dataretention>0:
                            sql = 'delete from ' + self.objectname + ' where trunc(' + str(
                                operationvariable['DATARETENTIONCOL']) + ') < trunc(sysdate-' + str(dataretention) + ')'
                            self.logobj.debug(' Execution method : ' + __name__ + self.step_exec_msg + str(
                                step_exec_id) + ' sql for deleting data :' + str(sql))
                            oracleobj._cursor.execute(str(sql))
                        else:
                            if len(self.objectname.split('.')) > 1:
                                oracleobj1 = self.factobj.getfactory('oracle', self.objectname.split('.'[0]))
                                sql = 'truncate table ' + self.objectname
                                self.logobj.debug(' Execution method : ' + __name__ + self.step_exec_msg + str(
                                        step_exec_id) + ' sql for truncating data :' + str(sql))
                                oracleobj1._cursor.execute(str(sql))
                                oracleobj1._cursor.close()
                            else:
                                sql = ' truncate table ' + self.objectname
                                self.logobj.debug(' Execution method : ' + __name__ + self.step_exec_msg + str(
                                     step_exec_id) + ' sql for truncating data :' + str(sql))
                                oracleobj._cursor.execute(str(sql))
                    cursorcolumndesc = self.get_cursor_cols(oracleobj, self.objectname, insert_cols)
                    if len(result) > 0:
                        if type(result) is pd.core.frame.DataFrame and yamlobj.variableimputer(task, operationvariable,
                                                                                               'target.db.loadoperation',
                                                                                               parameters) == 'UPSERT':
                           result = result[insert_cols.spilit('.')]
                           result = metaobj.convertdfNaNtoNone(result)
                        elif type(result) is pd.core.frame and yamlobj.variableimputer(task, operationvariable,
                                                                                       'target.db.loadoperation',
                                                                                       parameters) == 'DELINSERT':
                            ins_result = result[insert_cols.split('_')]
                            ins_result = metaobj.convertdfNaNtoNone(result)
                            del_result = result[delete_cols.split('_')]
                            del_result = metaobj.convertdfNaNtoNone(del_result)
                        elif targetcolumnlist is not None:
                             result = result[insert_cols.split('_')]
                             result = metaobj.convertdfNaNtoNone(result)
                        else:
                             result = metaobj.convertdfNaNtoNone(result)
                        commitfrequency = yamlobj.variableimputer(task, operationvariable, 'target.db.commitfrequency',
                                                                                       parameters)
                        if yamlobj.variableimputer(task, operationvariable, 'target.db.loadoperation',parameters) == 'UPSERT':
                            rowcount = oracleobj.dftodbinsert(meresql, result, cursorcolumndesc, commitfrequency)
                        elif yamlobj.variableimputer(task, operationvariable, 'target.db.loadoperation',
                                                                                       parameters) == 'DELINSERT':
                            cursorcolumndeldesc = self.get_cursor_cols(oracleobj, self.objectname, delete_cols)
                            rowcount = oracleobj.dftodbinsert(deletesql, del_result, cursorcolumndeldesc, commitfrequency)
                            rowcount = oracleobj.dftodbinsert(insertsql, ins_result, cursorcolumndesc, commitfrequency)
                        else:
                            rowcount = oracleobj.dftodbinsert(insertsql, result, cursorcolumndesc, commitfrequency)

                        self.logobj.info(self.step_exec_msg + str(step_exec_id) + self.step_end_msg + __name__)

                    else:
                        rowcount = 0
                    value = metaobj.configreaderexit('AUTO_STATSGATHER',  'enable', 'FALSE')
                    statsgatherflag = yamlobj.variableimputer(task, operationvariable, 'target.db.statsgather',parameters)
                    if (value == 'TRUE' and metaobj.Nonetostring(statsgatherflag,
                                                                 'None').upper() == 'NONE') or metaobj.Nonetostring(
                       statsgatherflag, '').upper() == 'Y':
                        if len(self.objectname.split('.')) > 1:
                           try:
                               oracleobj._cursor.close()
                           except (cx_Oracle.OperationalError, cx_Oracle.DatabaseError, cx_Oracle.InterfaceError):
                               pass
                           oracleobj = self.factobj.getfactory('oracle', self.objectname.split('.')[0])
                           gatherstatsql = metaobj.statsgathercreator(self.objectname.split('.')[0],
                                                                      self.objectname.slit('.')[1])
                           oracleobj._cursor.execute(gatherstatsql)
                        else:
                            oracleobj._cursor=oracleobj._connection.cursor()
                            gatherstatsql = metaobj.statsgathercreator(oracleobj.user, self.objectname)
                            oracleobj._cursor.execute(gatherstatsql)
                    try:
                        oracleobj._cursor.close()
                    except (cx_Oracle.OperationalError, cx_Oracle.DatabaseError, cx_Oracle.InterfaceError):
                        pass
                    return rowcount
                except Exception as e:
                     exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                     self.logobj.error(' Error Message : ' + str(exec_info))
                     raise

    def get_cursor_cols(self, oracleobj, objectname, insert_cols):
                cursorquery = 'select ' + insert_cols + ' FROM ' + objectname + ' where rownum < 2'
                oracleobj._cursor.execute(str(cursorquery))
                return oracleobj._cursor.description















