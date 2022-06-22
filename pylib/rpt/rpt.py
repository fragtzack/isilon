import logging
import pprint
import sys
import os
import datetime
import socket
import itertools

import util
import rpt

log = logging.getLogger()

class Rpt():
    """Rpt class provides methods to build and hold a report.
         Primary use case is for sending a report via email.

         Parameters:
             opts = a dictionary that contains the following:
                    daily_rpt_dir  = ../var/db/Daily_Reports usually
                    cifs_daily_rpt_dir = windows path  to the 
                                         daily_rpt_dir specified above
                    cifs_conf_dir = windows share path to the ../etc
                    mailhost = smtp relay server
                    mailport = optional port # for mailhost, default 25
                    email_from : The sender name for email 
                    email_subject : email subject
                    email_to : list of strings with email addresses
         Attributes:
             mailhost    = SMTP server
             mailport    = SMTP server port
             mailsubject = Email subject
             mailmessage = Message body
             mailto      = list of email addresses
             efrom       = email from address.(from is reserved word)
         Methods:
             email_prep()
             css_header()
             add_attachment()
             send_email()
    """
   
    ####################################################################
    def __init__(self,opts=None):
    ####################################################################
        log.debug("Initializing Rpt object)")
        if not opts:
             log.error("rpt.Rpt object requires opts object passed")
             return None
        if not opts.shortscript:
             log.error("rpt.Rpt opts object requires shortscript entry")
             return None
        self.opts=opts
        self.daily_rpt_dir = getattr(opts,'daily_rpt_dir','../var/Daily_Reports')
        self.daily_rpt_dir = self.daily_rpt_dir + '/' + self.opts.shortscript
        os.makedirs(self.daily_rpt_dir, exist_ok=True)
        today = datetime.date.today()
        datefile = today.strftime('%d%b%Y')
        ##tmpname = The tmp_dir name full path.
        ##                Note, the extension still needs to be added
        ##dailyname = The daily report name full path.
        ##                Note, the extension still needs to be added
        ##dailydatename = The daily report name full path with the data added.
        ##                Note, the extension still needs to be added
        self.tmpname = (self.opts.tmp_dir + '/' + opts.shortscript)
        self.dailyname = (self.daily_rpt_dir + '/' + opts.shortscript)
        self.dailydatename = (self.daily_rpt_dir + '/' + datefile + opts.shortscript)
        self.attachments=[]
        self.email_prep()
    ####################################################################
    def email_prep(self):
    ####################################################################
        """Prep the email to be sent."""
        self.mailhost = getattr(self.opts,'mailhost','mailhost_goes_here')
        self.mailport = getattr(self.opts,'mailport',25)
        self.mailsubject = getattr(self.opts,'email_subject','subject goes here')
        try:
            delattr(self.opts,'email_to')
        except AttributeError:
            pass
        self.mailto = getattr(self.opts,'email_to',[])
        # next line is broke due to bug with Python and 
        # running script without tty:
              ## self.currentuser = os.getlogin()
        self.currentuser = 'root'
        hostnamefqdn = None
        if socket.gethostname().find('.')>=0:
            hostnamefqdn=socket.gethostname()
        else:
            hostnamefqdn=socket.gethostbyaddr(socket.gethostname())[0]
        self.currentuser = self.currentuser + "@" + hostnamefqdn
        self.mailfrom = getattr(self.opts,'email_from',self.currentuser)
        now = util.unixtime()
        self.mailsubject = "{} {}".format(self.mailsubject,now)
        #print("subject = {}".format(subject))
        alist = ""
        if hasattr(self.opts,'append_email_to') \
           and not hasattr(self.opts,'noappendmail'):
           alist = self.opts.append_email_to.split(',')
           for mail in alist:
               #self.opts.email_to.append(mail)
               self.mailto.append(mail)
        self.mailmessage = ""
        self.css_header()
        """Add css style footer to self.mailmessage"""
    ####################################################################
    def css_header(self):
    ####################################################################
        """\nAdd css style header to self.mailmessage"""
        try:
            cssfile = self.opts.styleheader
        except AttributeError as err:
            msg= "Missing styleheader specification self.opts object"
            log.error(err)
            return False
        try:
            with open(cssfile) as myfile:
                css=myfile.read()
        except FileNotFoundError as err:
            log.error(err)
            return False
        self.mailmessage += css
    ####################################################################
    def add_css_heading(self,msg=None,color='black'):
    ####################################################################
        if not msg:
            return False
        css_color = "SB_Heading" ##black
        if color != "black":
            css_color += color 
        m = """<p Class=\"""" + css_color 
        m += """\">""" + msg + "</p>"
        self.mailmessage +=  m
    ####################################################################
    def add_css_footer(self,fl=None):
    ####################################################################
        """\nAdds CSS footer text to mail.message from passed in list.
           fl = footer list
        """
        m = """<center><table class="SB_Footer_Table"><thead>\n"""
        for line in fl:
            m += "<tr>\n"
            m += line + "\n"
            m += "</tr>\n"
        m += "</center></table></thead>\n"
        self.mailmessage += m
        
    ####################################################################
    def add_css_table(self,data=[],header=[],label=None):
    ####################################################################
        """\nAdd css table to self.mailmessage.
             Required parameters:
                 data = list of lists.
                 header = column headers for data list of lists.
                 label = optional, add SB_Heading text before the table
        """
        msg = """
           <body leftmargin="0" topmargin="0" marginwidth="0" marginheight="0">\n
           <center><table class="SB_Table"><thead>\n
           <tr>\n
        """
        self.mailmessage += msg
         
        for col in header:
            #print(col)
            m = """<th Class="SB_Tableheading">""" + col
            m += "</th>\n"
            self.mailmessage += m
        self.mailmessage += "</tr>\n"
        self.mailmessage += "</thead>\n"
        self.mailmessage += "<tbody>\n"

        c = itertools.cycle("123")
        for row in data:
            self.mailmessage += "<tr>\n"
            rowcolor = next(c)
            for col in row:
                 #print(col)
                 #m = """<td class="SB_TableRow2" nowrap="">""" + col
                 m = """<td class="SB_TableRow""" + rowcolor
                 m += """" nowrap="">\n""" + col
                 m += "</td>\n"
                 self.mailmessage += m
            self.mailmessage += "</tr>\n"
        self.mailmessage += "</tbody></table></center>\n"
    ####################################################################
    def add_attachment(self,filename=None):
    ####################################################################
        """Add file as attachment to report to be emailed"""
        if not filename:
            log.error("add_attachment requires a file name")
            return False
        if not os.path.isfile(filename):
            log.error("{} not found".format(filename))
        self.attachments.append(filename)
    ####################################################################
    def send_email(self):
    ####################################################################
        """Send off the email."""
        if ('none'.lower() in self.mailto):
            log.info('No email sent because mail to contains "none"')  
            return
        if (not('@' in mail for mail in self.mailto)):
            log.info('No email sent because mail to did not contain "@"')  
            return
        em = rpt.Email(self.opts)
        em.mailhost = self.mailhost
        em.port = self.mailport
        em.subject = self.mailsubject
        em.message = self.mailmessage
        em.to = self.mailto
        em.efrom = self.mailfrom
        for att in self.attachments:
            em.attach(att)
        em.send()

__version__ = 0.15
###########################################################################
def history():
###########################################################################
    """\n0.10 Initial
         0.11 Changes for new mail.py version
         0.12 add_css_table()
         0.13 disabled self.currentuser = os.getlogin()
         0.14 append_email_to now looks for noappendemail
         0.15 does not send mailif self.mailto = None
    """
    pass

