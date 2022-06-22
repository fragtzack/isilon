import logging
import pprint
import sys
import os
import types
import xlsxwriter

import util

log = logging.getLogger()

####################################################################
class Workbook(xlsxwriter.Workbook):
####################################################################
    """Spreadsheet class provides methods to build a spreadsheet.
    """
   
    ####################################################################
    def __init__(self,filename=None, options={}):
    ####################################################################
        if not filename.endswith('.xlsx'):
            filename = filename + ('.xlsx')
        try:
            filename = util.fname(filename)
        except FileNotFoundError as err:
            log.error(err)
            return False
        self.filename = filename
        log.info("Initializing excel file " + self.filename)
        super().__init__(self.filename,options)
    ####################################################################
    def add_tab(self,name=None,data=None,header=None,legend=None,
                srow=0,scol=0,frow=1,fcol=1,autocol=True):
    ####################################################################
        """Provides method to add_worksheet and add_table in 1 call.
 
             Required Attribute args:
                 name = TAB name
                 header = list of header names
                 data = list of lists for spreadsheet contents

             Optional Attribute args:
                 legend = List of descriptions for columns. Will
                          be added as a comment. Legend list count must match
                          the header list count.
                 srow = starting row for table, default 0
                 scol = starting col for table, default 0
                 frow = Freeze pane row specify
                 fcol = Freeze pane col specify
                 autocol = True/False, auto set the column sizes
                 

             add_tab also adds the worksheet.header attribute to
             allow the set_col_by_name function to work
        """

        if not data:
            log.warning("data=[][] required")      
            return None
        if not header:
            log.warning("header=[] required")      
            return False
        columns = []
        for field in header:
            columns.append({ 'header' : field })
        worksheet = self.add_worksheet(name)
        worksheet.header = header
        data = type_fix(data)
        tableinfo= {
            'data' : data,
            'columns' : columns
            }
        lastcol = scol + (len(header) - 1)
        #lastrow = srow + (len(data) + 1)
        lastrow = srow + len(data)

        worksheet.add_table(srow,scol,lastrow,lastcol,tableinfo)
        worksheet.freeze_panes(frow,fcol)
        ##monkey patch to xlsxwriter.Worksheet)
        worksheet.set_col_by_name = types.MethodType( set_col_by_name, worksheet)
        if autocol:
            worksheet.auto_set_columns = types.MethodType( auto_set_columns, worksheet )
            worksheet.auto_set_columns(data=data,scol=scol)
        if legend:
            worksheet.add_legend = types.MethodType( add_legend, worksheet )
            worksheet.add_legend(header=header,legend=legend)
        return worksheet
    ####################################################################
    def add_formats(self,options={}):
    ####################################################################
        format_head = self.add_format()
        format_head.set_bold()
        format_head.set_color('white')
        format_head.set_bg_color('gray')
        format_head.set_align('center')
####################################################################
def set_col_by_name(self,colname=None,width=10,
                    header=None,scol=0):
####################################################################
    """Sets a column width by header name.

         Required Attribute args:
             self = worksheet object
             colname = column name to set, case sensitive
             width = column width to set
         Optional Attribute args:
             scol = Column start
             header = row of headers for data list of lists.
                      If header not specified, worksheet 
                      must have been created with self.add_tab()
    """
    colnum = self.header.index(colname) + scol
    #log.info("Tab =>{} Setting colname =>{}, column =>{}, to width =>{}".
                #format(self.get_name(),colname,colnum,width))
    self.set_column(colnum,colnum,width)
####################################################################
def add_legend(self,header=None,legend=None):
####################################################################
    """Adds a description as a comment to each header column name.
       header and legend must be a lists. Both lists of the same size.
    """
    log.info("Adding comments to column headers for tab {}".format(self.get_name()))
    if (len(header) != len(legend)):
        log.error("Header and Legend lists must be same length")
        return
    for idx,val in enumerate(legend):
        self.write_comment(0,idx,val)
 
####################################################################
def auto_set_columns(self,data=None,header=None,scol=0):
####################################################################
    """Determines the max length of each column and then set  
          that column width. 

         Required Attribute args:
             data = list of lists data
         Optional Attribute args:
             scol = Column start
             header = row of headers for data list of lists.
                      If header not specified, worksheet 
                      must have been created with self.add_tab()
    """
    if not header and self.header:
        header = self.header

    ## table = [] list of lists, combine header and data
    table = []
    table.append(header)
    for row in data:
        table.append(row)
    ziptable = list(zip (*table))
    colsizes = []
    for idx,val in enumerate(table[0]):
        colnum = idx + scol
        #print("Determine size column => {} col label => {}".format(colnum,val))
        #all elements must be type str to use max() for compare
        sl = list(map(str,ziptable[idx]))
        size = len(max(sl, key=len))
        size = size + 2.5
        log.debug("Setting column => {} col size => {} => {}".format(colnum,val,size))
        self.set_column(colnum,colnum,size)
####################################################################
def coerce(x):
####################################################################
    """Attempt to turn a str into an int or float, return type.
       Strips trailing/leading whitespace from str.
    """
    try:
        x.strip()
    except AttributeError: ##becasome strip only works with str
        return(x)
    try:
        a = float(x)
        b = int(x)
        if a == b:
            return b
        else:
            return a
    except:
        #print("Failed to coerce str to int or float")
        return str(x)
####################################################################
def type_fix(data=None):
####################################################################
    """Takes as input a list of lists. Strips leading and trailing
       whitespace. Checks each element for a 
       int or float. Changes type() to type discovered.
    """
    ndata = []
    for rn,row in enumerate(data):
        nrow = []
        for idx,val in enumerate(row):
            val = coerce(val)
            #print(idx,val,type(val))
            nrow.append(val)
        ndata.append(nrow)
    #sys.exit()
    #pprint.pprint(ndata);sys.exit()
    return(ndata)
__version__ = 0.31
###########################################################################
def history():
###########################################################################
    """0.10 Initial
       0.11 Fix for auto_set_columns change list elements to str() first
       0.20 def type_fix()
       0.30 add_legend
       0.31 set_col_by_name monkey patched to worksheet
    """
    pass

