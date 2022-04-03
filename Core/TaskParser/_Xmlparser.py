








import pandas as pd
import pickle as pk
import traceback
from xml.dom import minidom
import xlsxwriter
from Common.Logger._Metadata import _Metadata
from Common.Utilities._utils import _utils
import xml.etree.ElementTree as XML
from Common.Logger.logger import logger
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import pandas as pd
from lxml import etree,objectify
# from xml import etree,objectify

#from xml.etree import XMLSyntaxError
import os

class CustomException(Exception):
    pass
class CustXMLValidationError(CustomException):
    def __init__(self,value):
        self.value=value
    def __str__(self):
        return(repr(self.value))

class _Xmlparser():
    def __init__(self,workflowname):
        self.factobj = _utils()
        self.log = logger(workflowname)
        self.logobj = self.log.getcustomlogger()
        self.step_exec_msg = ' step_exec_id : '
        self.step_start_msg = 'Started execution of : '
        self.step_end_msg = ' Ended execution of : '
        self.topdict = []
        self.taglist=[]


    def parse_root(self,root,tags,rootcol,elemlist,xmltagelementjoin):
        """Return a list of dictionaries from the text and attributes of the
        childre under this XML root."""
        try:
            return [self.parse_element(child,root,tags,elemlist,xmltagelementjoin, None) for child in root.findall(rootcol)]
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise


    def process_data(self,root,tags,rootcolumn,elemlist,xmltagelementjoin):
        """Initiate the root XML, parse it, and return a dataframe"""
        try:
            structure_data = self.parse_root(self,root,tags,rootcolumn,elemlist,xmltagelementjoin)
            return pd.DataFrame(self.topdict, columns=tags)
        except Exception as e:
            exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            self.logobj.error(' Error Message : ' + str(exec_info))
            raise

    def parse_element(self,element,root,xmltags,rootcolumn,elemlist,xmltagelementjoin,parsed=None):
         """Collect (key:attribute) and (tag:text) from thie XML
         element and all its children into a single dictonary of strings."""
         try:
             recdic = []
             if parsed is None:
                 parsed = dict()

             if len(self.taglist) == 0:
                 for tags in xmltags:
                     llll = tags.split(xmltagelementjoin)[0]
                     self.taglist.append(111)

             if element.tag in self.taglist:
                 for tags in xmltags:
                     ll = tags.split(xmltagelementjoin)
                     if ll[0] == element.tag:
                         if ll[1] in element.keys():
                             parsed[tags] = element.attrib.get(ll[1])
                         elif element.text is not None and ll[1].upper()=='TEXT':
                             parsed[tags] = element.text
                         else:
                             parsed[tags] = None
                     elif ll[0] == 'root':
                         parsed[tags] = root.attrib.get(ll[1])
                     else:
                         if len(element.getchildren()) == 0:
                             parsed[tags] = parsed[tags]
                 if len(element.getchildren()) == 0:
                     for k, v in parsed.items():
                         recdic.append(v)
                     self.topdict.append(recdic)
                     parsed = dict()

             """ Apply recursion"""
             for child in element.getchildren():
                  self.parse_element(child,root,xmltags,elemlist,xmltagelementjoin, parsed)
         except Exception as e:
             exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
             self.logobj.error(' Error Message : ' + str(exec_info))
             raise

         def validate(self,xml_path: str, xsd_path: str) -> bool:
             xmlschema_doc = etree.parse(xsd_path)
             xmlschema = etree.XMLSchema(xmlschema_doc)
             xml_doc = etree.parse(xsd_path)
             result = xmlschema.validate(xml_doc)
             return result

    def readparser(self,task,operationvariable,step_exec_id,pretask_df=None,workflowname=None,argdict=None,parameters=None):
        try:
            return_val=()
            xmlfilename=[]
            self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
            self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) + 'parameter for xml readfile are')
            yamlobj = self.factobj.getfactory('yamlparser')
            metaobj = _Metadata()
            datasharing = metaobj.configreader('DATASHRARING_PATH', 'file_path')
            sourcefilepath = metaobj.configreader('SOURCE_PATH', 'file_path')
            delimiter = metaobj.configreader('DELIMITERFORPATH', 'delimiter')
            columnlist=yamlobj.variableimputer(task, operationvariable, 'source.file.xmlcollist', parameters)
            rootcolumn = yamlobj.variableimputer(task, operationvariable, 'source.file.rootelement', parameters)
            xsdschema=yamlobj.variableimputer(task, operationvariable, 'source.file.xsdschema', parameters)
            xsdschemafolder=yamlobj.variableimputer(task, operationvariable, 'source.file.xsdschemafolder', parameters)
            if xsdschemafolder is not None:
                xsdschemapath = metaobj.configreader(xsdschemafolder, 'file_path')
                xsdschema=xsdschemapath+delimiter+xsdschema
            xmltagelementjoin=yamlobj.variableimputer(task, operationvariable, 'source.file.xmltagelementjoin', parameters)
            path = yamlobj.variableimputer(task, operationvariable, 'source.file.filepath', parameters)

            if yamlobj.variableimputer(task, operationvariable, 'source.file.filepath', parameters):
                readtype=yamlobj.variableimputer(task, operationvariable, 'source.file.folderreadtype', parameters)
                path= metaobj.configreader(path, 'file_path')
                if readtype.upper()=='ALL':
                    xmlfilename = metaobj.get_file(path, readtype)
                elif readtype is not None:
                    xmlfilename =metaobj.get_file(path, readtype)
            else:
                filename = yamlobj.variableimputer(task, operationvariable, 'source.file.filename', parameters)
                filename=str(path) + filename
                xmlfilename = list(filename)

            columnlist=[column for column in columnlist.split('_')]
            xmltags=columnlist
            df=pd.DataFrame(self.topdict, columns=xmltags)
            elemlist=[]
            for xmlfile in xmlfilename:
                if self.validate(xmlfile,xsdschema):
                    xml = objectify.parse(xmlfile)
                    for elem in xml.iter():
                        elemlist.append(elem.tag)
                    elemlist=list(set(elemlist))
                    self.topdict = []
                    df2=self.process_data(xml.getroot(),xmltags,rootcolumn,elemlist,xmltagelementjoin)
                    df=df.append(df2,ignore_index=True)
                else:
                    raise CustXMLValidationError("Xml is not validated Successfully")
                datasharingfile = datasharing + str(step_exec_id) + '_' + yamlobj.variableimputer(task, operationvariable,
                                                    'source.file.sourceidentifier', parameters) + '_SOURCE.PICKLE'
                pickling_on = open(datasharingfile, "wb")
                pk.dump(df, pickling_on)
                pickling_on.close()
                xmlpickle = str(step_exec_id) + '_' + yamlobj.variableinputer(task, operationvariable,
                                            'source.file.sourrceidentifier',parameters) + '_SOURCE.PICKLE'
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                return_val['picklefile'] = xmlpickle
                return_val['recordcount'] = len(df)
                return_val['STATUS']='S'
                return return_val
        except Exception as e:
                exec_info = ''.join(traceback.format_exception(etype=(e), value=e, tb=e.__traceback__))
                self.logobj.error(' ErrorMessage : '+ str(exec_info))
                raise

        def writefile(self,task,operationvariable,step_exec_id,result=None,workflowname=None,argdict=None,parameters=None):
            try:
                self.logobj.info(self.step_start_msg + __name__ + self.step_exec_msg + str(step_exec_id) )
                self.logobj.debug(' Executing method : ' + __name__ + self.step_exec_msg + str(step_exec_id) + 'parameter for xml readfile are')
                yamlobj = self.factobj.getfactory('yamlparser')
                metaobj = _Metadata()
                datasharing = metaobj.configreader('DATASHRARING_PATH', 'file_path')
                sourcefilepath = metaobj.configreader('SOURCE_PATH', 'file_path')
                delimiter = metaobj.configreader('DELIMITERFORPATH', 'delimiter')
                columnlist=yamlobj.variableimputer(task, operationvariable, 'target.file.xmlcollist', parameters)
                rootcolumn = yamlobj.variableimputer(task, operationvariable, 'target.file.rootelement', parameters)
                xsdschema=yamlobj.variableimputer(task, operationvariable, 'target.file.xsdschema', parameters)
                xsdschemafolder=yamlobj.variableimputer(task, operationvariable, 'target.file.xsdschemafolder', parameters)
                filepath=yamlobj.variableimputer(task, operationvariable, 'target.file.filepath', parameters)
                filename = yamlobj.variableimputer(task, operationvariable, 'target.file.filename', parameters)
                inputsource = yamlobj.variableimputer(task, operationvariable, 'target.file.inputsource', parameters)
                elementseparator = yamlobj.variableimputer(task, operationvariable, 'target.file.elementseprator', parameters)
                groupingcols=yamlobj.variableimputer(task, operationvariable, 'target.file.groupingcols')
                result=  result['PICKLEFILE']
                if 'PICKLE' in result:
                    datasharefile = datasharing + result
                    pickle_off = open(datasharefile, "rb")
                    result = pk.load(pickle_off)
                    result = result[inputsource]
                root=Element(rootcolumn)
                columnlistsplit=[]
                for splitindex in columnlist.split(','):
                    if 'root' in splitindex:
                        root.set(splitindex.split(elementseparator)[1], str(result[splitindex[0]]))
                elementlist = []
                elementdict=()
                prerow=None
                for i in columnlist.split(','):
                    if 'root' not in i:
                        elementdict[i]=len(i.split(elementseparator))
                listofTuples = sorted(elementdict.items(), key=lambda x: x[1])
                for elem in listofTuples:
                    columnlistsplit.append(elem[0])
                removecollist=[]
                for index,row in result.itterrows():
                    newcolumnlist=columnlistsplit
                    if prerow is not None:
                        duplicatecol=list(row[row==prerow].index)
                        removecol=[col for col in duplicatecol if col in groupingcols.split(',')]
                        if len(removecol) >0:
                            removecollist=set(removecol)
                            elementlist=[]
                            for x in removecollist:
                                elementlist.append(metaobj.xmlwriterlistappender(x, elementseparator,elementseparator))
                            [newcolumnlist.remove(x) for x in removecollist if x in columnlistsplit]
                            newcolumnlist=metaobj.listsorterbaseduponelemlength(list(newcolumnlist),elementseparator)
                        else:
                            newcolumnlist=columnlistsplit
                            elementlist=[]
                    for col in newcolumnlist:
                        if elementseparator is not None:
                           if len(col.split(elementseparator))>1:
                               colheriarcy= col.split(elementseparator)
                               for colindex in range(len(colheriarcy)):
                                   if colindex== len(colheriarcy)-1:
                                      if colheriarcy[colindex]=='text':
                                          newelement.text=row[col]
                                      else:
                                          newelement.set(colheriarcy[colindex],str(row[col]))
                                   else:
                                      if colindex==0:
                                          colheriarcy1=colheriarcy[colindex]
                                      else:
                                          colheriarcy1=colheriarcy+elementseparator+colheriarcy[colindex]
                                      if colheriarcy1 not in elementlist:
                                          if colindex==0:
                                              childtr=SubElement(root,colheriarcy1)
                                          else:
                                               xpath=metaobj.xmlwriterlistappender(colheriarcy1, elementseparator, '/')
                                               childtr = SubElement(root.findall(xpath)[-1],colheriarcy[colindex])
                                          elemlist.append(colheriarcy1)
                                      newelement=root.findall(colheriarcy1.replace(elementseparator,'/'))[-1]
                    prerow=row
                tree=XML.ElementTree(root)
                file=filepath+delimiter+filename
                with open(file,"wb") as file_handle:
                    tree.writer(file_handle)
                file_handle.close()
                if xsdschemafolder is not None:
                    xsdschemapath = metaobj.configreader(xsdschemafolder, 'file_path')
                    xsdschema=xsdschemapath+delimiter+xsdschema
                if xsdschema is not None:
                    if self.validate(file,xsdschema):
                       None
                    else:
                       raise CustXMLValidationError("Xml is not validated Successfully")
                self.logobj.info(self.step_end_msg + __name__ + self.step_exec_msg + str(step_exec_id))
                return 1
            except Exception as e:
                exec_info = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
                self.logobj.error(' Error Message : ' + str(exec_info))
                raise





















