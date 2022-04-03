

import os
import pickle as pk
import traceback

from Common.Logger._Metadata import _Metadata
import re

from  Common.Utilities._utils import _utils
from Common.Logger.logger import logger
import datetime
class _mgparser():
    def __init__(self,workflowname):
        self.factobj=_utils()
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg=' step_exec_id : '
        self.step_start_msg=' Started execution of : '
        self.step_end_msg = ' Ended execution of : '
    def reading(self,task,operationvariable,step_exec_id,pretask_df,workflow=None,argdict=None,parameters=None):
        try:
            running=True
            reccount-0
            return_val = []
            self.logobj.info(self.step_start_msg + __name__+self.step_exec_msg + str(step_exec_id) )
            self.logobj.debug(' Executing method : ' + __name__ +self.step_exec_msg + str(step_exec_id) +
                              'parameter for mq read are task :'+str(task)+' operationvariable :'+str(operationvariable))
            yamlobj = self.factobj.getfactory('yamlparser')
            metaobj = _Metadata()
            datashringpath = metaobj.configreader('DATASHARING_PATH', 'file_path')
            appsignerpath  = metaobj.configreader('APPSIGNER_PATH', 'file_path')
            delimiter = metaobj.configreader('DELIMITERFORPATH', 'delimiter')
            if yamlobj.variableimputer(task, operationvariable, 'source.queue.queue.mqconnection', parameters)=='mq':
                mqconnection = yamlobj.variableimputer(task, operationvariable, 'source.queue.mqconnection', parameters)
                if mqconnection is not None:
                    queuemanager = metaobj.configreader(mqconnection, 'queuemanager')
                    channel = metaobj.configreader(mqconnection, 'channel')
                    host = metaobj.configreader(mqconnection, 'host')
                    port = metaobj.configreader(mqconnection, 'port')
                    queuename = metaobj.configreader(mqconnection, 'queuename')
                    sslcipherspec = metaobj.configreader(mqconnection, 'sslcipherspec')
                    scheduletime = metaobj.configreader(mqconnection, 'scheudulestoptime')
                    window = metaobj.configreader(mqconnection, 'stopwindow')
                else:
                    queuemanager=yamlobj.variableimputer(task, operationvariable, 'source.queue.queuemanager', parameters)
                    channel=yamlobj.variableimputer(task, operationvariable, 'source.queue.channel', parameters)
                    host=yamlobj.variableimputer(task, operationvariable, 'source.queue.host', parameters)
                    port=yamlobj.variableimputer(task, operationvariable, 'source.queue.port', parameters)
                    queuename=yamlobj.variableimputer(task, operationvariable, 'source.queue.queuename', parameters)
                    sslcipherspecter(task, operationvariable, 'source.queue.sslcipherspec', parameters)
                conn_info = "%s(%s)" % (host, port)
                conn_info1 = conn_info.encode()
                key_repo_location = os.path.realpath(os.path.join(os.path.dirname(__file__),appsignerpath+delimiter+
                                                                  'Application_Signer'))
                key_repo_location = key_repo_location.encode()
                ssl_cipher_spec = sslcipherspec.encode()
                cd = pymqi.CD()
                cd.MaxMsgLength=104857
                cd.Channel1Name = channel.encode()
                cd.ConnectionName = conn_info1
                cd.ChannelType = CMQC.MQCHT_CLNTCONN
                cd.TransportType = CMQC.MQXPT_TCP
                cd.SSLCipherSpec = ssl_cipher_spec
                os.environ['AMQ_SSL_OCSP_NO_CHECK_AIA'] = "1"
                sco = pymqi.SCO()
                sco.KeyRepository = key_repo_location
                gmo=pymqi.GMO
                gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
                gmo.WaitInterval = 5000
                gmqr = pymqi.QueueManager(None)
                qmqr.connect_with_options(queuemanager, cd, sco)
                get_queue = pymqi.Queue(gmqr, queuename)
                if str(yamlobj.variableimputer(task, operationvariable, 'source.queue.messagetype', parameters)).upper()\
                    in ('XML','FILE') and scheduletime is not None:
                   while running:
                       if __name__ == '__main__':
                           try:
                               message=get_queue.get()
                               message=message.decode("utf-8",errors="ignore")
                               if str(yamlobj.variableimputer(task, operationvariable,
                                                              'source.queue.messagetype',parameters)).upper() == 'XML':
                                   mqmessage_path=yamlobj.variableimputer(task, operationvariable, 'source.queue.mqmessagepath',parameters)
                                   mqmessagepath = metaobj.configreader(mqmessage_path, 'file_path')
                                   reccount = reccount + 1
                                   xmlsavepath=mqmessagepath+delimiter+str(step_exec_id)+'_'+str(reccount)+'_'+\
                                               str(yamlobj.variableimputer(task,operationvariable,
                                                                           'source.queue.sourceidentifier',parameters))+'.xml'
                                   fileobj=open(xmlsavepath,'w')
                                   fileobj.write(message)
                                   fileobj.close()
                                   self.logobj.info(self.step_start_msg + __name__ +self.step_exec_msg +
                                                    str(step_exec_id)+'A new file '+xmlsavepath+ ' has been recieved ')
                                   self.logobj.debug('A new file '+xmlsavepath + ' has been recieved Executing method:'
                                        + __name__ + self.step_exec_msg + str(step_exec_id) + 'parameter for mq read are task :' + str(
                                       task) + ' operationvariable :' + str(operationvariable))
                                   datashringfile = datashringpath + str(step_exec_id) + '_' + \
                                                    yamlobj.variableimputer(task,operationvariable,'source.queue.sourceidentifier'
                                                                            ,parameters)  + '_SOURCE.PICKLE'
                                   pickling_on = open(datashringfile, "wb")
                                   pk.dump(message, pickling_on)
                                   pickling_on.close()
                                   mqpickle = str(step_exec_id) + '_' + yamlobj.variableimputer(task, operationvariable,
                                                                                               'source.queue.sourceidentifier',
                                                                                                parameters)  + '_SOURCE.PICKLE'
                                   return_val['picklefile'] = mqpickle
                                   return_val['recordcount'] = reccount
                           except pymqi.MQMIError as e:
                               if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                                   pass
                               else:
                                   raise
                           currentdatetime = datetime.datetime.now()
                           currenthour = currentdatetime.hour
                           currentminutes = currentdatetime.minute
                           currentsecond = currentdatetime.second
                           if int(currenthour) == int(scheduletime.spilit(':')[0]) and (
                                   int(currentminutes) >= int(scheduletime.spilit(':')[1]) and int(currentminutes) < int(
                              scheduletime.spilit(':')[1]) + int(window)):
                              running = False
                       if not running:
                           if reccount>0:
                               return return_val
                           else:
                               datashringfile = datashringpath + str(step_exec_id) + '_' +\
                                                yamlobj.variableimputer(task,operationvariable,'source.queue.sourceidentifier',parameters)
                               pickling_on = open(datashringfile, "wb")
                               pk.dump('Blank MQ Message', pickling_on)
                               pickling_on.close()
                               mqpickle = str(step_exec_id) + '-' + yamlobj.variableimputer(task, operationvariable,
                                                                                            'source.queue.sourceidentifier',
                                                                                            parameters) + '_SOURCE.PICKLE'
                               return_val['picklefile'] = mqpickle
                               return_val['recp\ordcount'] = 0
                               return_val['STSTUS']= 'S'
                               return return_val
                elif str(yamlobj.variableimputer(task, operationvariable, 'source.queue.messagetype',
                                                 parameters)).upper() =='TEXT_MESSAGE':
                    return None
                    while running:
                        try:
                            message=get_queue.get()
                            message=message.decode("utf-8",errors="ignore")
                            pattern = yamlobj.variableimputer(task, operationvariable, 'source.queue.pattern',
                                                              parametrs)
                            result = re.match(pattern, message)
                        except pymqi.MQMIError as e:
                            if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                                pass

                            else:
                                raise
                        if result:
                            datashraingfile = datasharingpath + str(step_exec_id) + '_' + yamlobj.variableimputer(task
                                                  ,operationvariable,'source.queue.sourceidentifier',parameters) + '_SOURCE.PICKLE'
                            pickling_on = open(datashraingfile, "wb")
                            pk.dump(message, pickling_on)
                            pickling_on.close()
                            mqpickle = str(step_exec_id) + '_' + yamlobj.variableimputer(task, operationvariable,
                                                                                         'source.queue.sourceidentifier',
                                                                                         parameters) + '-SOURCE.PICKLE'
                            return_val['picklefile'] =mqpickle
                            return_val['recordcount'] = 1
                            return_val['STATUS']='S'
                            running = False
                            return return_val

        except Exception as e:
               exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
               self.logobj.error(' Error Message : ' + str(exec_info))
               raise
    def writemq(self,task,operationvariable,step_exec_id,result,workflowname=None,argdict=None,parameters=None):
        try:
            running=True
            reccount=0
            return_val = []
            self.logobj.info(self,step_start_msg + __name__+self.step_exec_msg + str(step_exec_id))
            self.logobj.debug(' Executing method : ' + __name__ +self.step_exec_msg + str(step_exec_id)  +
                              'parameter for mq read are task :'+str(task)+' operationvariable :'+str(operationvariable))
            yamlobj = self.factobj.getfactory('yamlparser')
            metaobj = _Metadata()
            datasharingpath = metaobj.configreader('DATASHARING_PATH', 'file_path')
            appsignerpath = metaobj.configreader('APPSIGNER_PATH', 'file_path')
            delimiter = metaobj.configreader('DELIMITERFORPATH', 'delimiter')
            if yamlobj.variableimputer(task, operationvariable, 'source.queue.queue.mqconnection', parameters) == 'mq':
                mqconnection = yamlobj.variableimputer(task, operationvariable, 'source.queue.mqconnection', parameters)
            if mqconnection is not None:
                queuemanager = metaobj.configreader(mqconnection, 'queuemanager')
                channel = metaobj.configreader(mqconnection, 'channel')
                host = metaobj.configreader(mqconnection, 'host')
                port = metaobj.configreader(mqconnection, 'port')
                queuename = metaobj.configreader(mqconnection, 'queuename')
                sslcipherspec = metaobj.configreader(mqconnection, 'sslcipherspec')
                scheduletime = metaobj.configreader(mqconnection, 'scheudulestoptime')
                window = metaobj.configreader(mqconnection, 'stopwindow')
            else:
                queuemanager = yamlobj.variableimputer(task, operationvariable, 'source.queue.queuemanager', parameters)
                channel = yamlobj.variableimputer(task, operationvariable, 'source.queue.channel', parameters)
                host = yamlobj.variableimputer(task, operationvariable, 'source.queue.host', parameters)
                port = yamlobj.variableimputer(task, operationvariable, 'source.queue.port', parameters)
                queuename = yamlobj.variableimputer(task, operationvariable, 'source.queue.queuename', parameters)
                sslcipherspecter(task, operationvariable, 'source.queue.sslcipherspec', parameters)
            conn_info = "%s(%s)" % (host, port)
            conn_info1 = conn_info.encode()
            key_repo_location = os.path.realpath(os.path.join(os.path.dirname(__file__), appsignerpath + delimiter +
                                                              'Application_Signer'))
            key_repo_location = key_repo_location.encode()
            ssl_cipher_spec = sslcipherspec.encode()
            cd = pymqi.CD()
            cd.MaxMsgLength = 104857
            cd.Channel1Name = channel.encode()
            cd.ConnectionName = conn_info1
            cd.ChannelType = CMQC.MQCHT_CLNTCONN
            cd.TransportType = CMQC.MQXPT_TCP
            cd.SSLCipherSpec = ssl_cipher_spec
            os.environ['AMQ_SSL_OCSP_NO_CHECK_AIA'] = "1"
            sco = pymqi.SCO()
            sco.KeyRepository = key_repo_location
            gmo = pymqi.GMO
            gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
            gmo.WaitInterval = 5000
            gmqr = pymqi.QueueManager(None)
            qmqr.connect_with_options(queuemanager, cd, sco)
            get_queue = pymqi.Queue(gmqr, queuename)
            if yamlobj.variableimputer(task, operationvariable, 'target.queue.messagetype', parameters).upper() \
                    not  in ('DF', 'TEXT_MESSAGE') :
                sourcefilepath=yamlobj.variableimputer(task, operationvariable, 'target.queue.sourcefilepath', parameters)
                sourcefilename = yamlobj.variableimputer(task, operationvariable, 'target.queue.sourcefilename', parameters)
                if sourcefilepath is not None:
                    filename=sourcefilename+delimiter+sourcefilename
                else:
                    filename=sourcefilename
            elif yamlobj.variableimputer(task, operationvariable, 'target.queue.messagetype', parameters).upper()=='TEXT_MESSAGE':
                messagetext=yamlobj.variableimputer(task, operationvariable, 'target.queue.messagetext', parameters)
                result=messagetext
            else:
                inputsource=yamlobj.variableimputer(task, operationvariable, 'target.queue.inputsource', parameters)

                if 'PICKLE' in result:
                    datasharingfile = datasharingpath + result
                    pickle_off = open(datasharingfile, "rb")
                    result = pk.load(pickle_off)
                    result = result[inputsource]
            if yamlobj.variableimputer(task, operationvariable, 'target.queue.messagetype', parameters).upper() in ('XML', 'TEXT'):
                with open(filename,'r') as reader:
                    msg=reader.read()
                reader.close()
            else:
                msg=str(result)
                msg=msg.encode()
                get_queue.put(msg)
                get_queue.close()
                gmqr.disconnect()
                return 1
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise





















