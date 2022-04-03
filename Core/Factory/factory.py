from Core.TaskParser._Source import _source
from Core.TaskParser._Target import _Target
from Core.TaskParser._System import _System

import traceback


class factory:
    def __init__(self,pipelinename=None):
        self.pipelinename=pipelinename

    def getfactory(self,tasktype):
        try:
            connectionobj={'source':_source(self.pipeline) ,'target':_Target(self.pipelinename), 'system': _System(self.pipelinename)
                ,'event': None}
            return connectionobj[tasktype]
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message :' + str(exec_info))
            raise
        