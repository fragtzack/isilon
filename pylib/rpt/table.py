import logging
import pprint
import sys
import os
import csv

import util

log = logging.getLogger()

class Table():
    """\nTable class provides methods to build and  hold 
         tables. Primary design to build a list of lists.

         Requires a list of headers as input.
         Attributes:
             self.header = list of the column headers
             self.data = list[] of lists[].
                      Generally used for generating spreadsheet tabs, 
                      csv files and html tables. Constucted by zipping
                      together all the column header lists.
         Methods:
             create_attribs()
                 Creates an empty list for each column header.
                 Allows to append value by column name. 
                 Example for 'size' column-> 
                              self.size.append('1024MB')
              
    """
   
    ####################################################################
    def __init__(self,header=None,**kargs):
    ####################################################################
        if not isinstance(header,list):
            log.error("Require paremeter header = list type")
            return False
        self.header = header
        self.create_attribs()
    ################################################################
    def create_attribs(self):
    ################################################################
        """\nCreates an empty list for each column header"""

        for col in self.header:
            setattr(self,col,[])
    ################################################################
    @property
    def freshdata(self):
    ################################################################
        """\n
           forces def data(self) to update _data and returns def data
        """

        delattr(self,'_data')
        return self.data
    ################################################################
    @property
    def data(self):
    ################################################################
        """\nMake and return list of lists from all the attribs
             created in self.create_attribs. 
             If column lengths are different, do not create/return
             list of lists, instead return None.
        """
        try:
            return getattr(self,'_data')
        except AttributeError:
            pass
        zipme = []
        cl = 0 #max column length
        for col in self.header:
            coldata = getattr(self,col)
            collen = len(coldata)
            #print(col,collen)
            if collen > cl and cl == 0:
               cl = collen
            if collen > cl and cl != 0:
               log.error("Column->{} is longer length than other columns."
                        .format(col))
               return []
            if collen < cl:
               log.error("Column->{} is shorter length than other columns."
                        .format(col))
               return []
            zipme.append(coldata)
        self._data = list(zip(*zipme))
        #self.data_frame(self._data)
        return self._data
    ################################################################
    @property
    def sheader(self):
    ################################################################
        """\n
           Returns self.header with a space appened to each col name.
           Helps with formating excel columns.
        """
        return [ '{} '.format(col) for col in self.header ]
    
    ################################################################
    def to_csv(self,csvfile):
    ################################################################
        """\n
             Dumps the self.data to a csvfile. csvfile names
             passed without .csv will get .csv added.
        """
        if not csvfile.endswith('.csv'):
            csvfile = csvfile + ('.csv')
        log.info("Dumping to csv file {}".format(csvfile))
        try:
            with open(csvfile , 'w') as f:
                writer = csv.writer(f)
                writer.writerow(self.header)
                writer.writerows(self.data)
                return True
        except FileNotFoundError as err:
                log.error(err)
                return False
__version__ = 0.30
###########################################################################
def history():
###########################################################################
    """\n
       0.10 Initial
       0.11 def data() now detects if column lengths are diff
       0.20 def freshdata
       0.30 def sheader (headers with space append formating changes)
    """
    pass

