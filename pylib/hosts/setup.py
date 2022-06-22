import os.path
import configparser
import itertools
import sys
import argparse
import logging
import pprint
import re
import copy
import io
import time

import hosts
import rpt

os.umask(0o002)
log = logging.getLogger()


###########################################################################
class SetOpts():
###########################################################################
    """\nConifgure an object that stores all the settings to be used when
       working with a "flavor" (vnx,netapp,isilon,atmos).

      Each of the objects gets copied to a __class__ attribute to allow
      for modules in other namespace outside of main to access the objects.
      Gets the .conf file settings, merges some settings from op (argv),
      performs sanity checking and overrides. The parameters .conf files
      passed do not include the .conf extension.
      In addition to the passed .conf file names, SetOps will also
      look for a .conf file associated with the script name
      Usual passed parms =
            op = object of the parsed sys argv settings
            main = main config file, should be 'hosts' usually
            flavor = vnx, netapp, atmos  or isilon
            script = True/False parse the script.conf file
            
    """
    #instances = []
    #opts = ""
    #main = ""
    #flavor = ""
    ########################################################################
    def __init__(self,opts=None,main=None,flavor=None,script=False):
    ########################################################################
        self.log = logging.getLogger()
        self.log.debug("Adding .conf info main=%s flavor=%s script=%s" 
                        % (main,flavor,script))
        scriptf = None # script file
        if script:
            scriptf = opts.shortscript
        odict=parse_config_files(main=main,flavor=flavor,script=scriptf)
        if not odict :
            if flavor:
                self.hosts = []
            return None
        for key,value in odict.items():
            setattr(self,key,value)
        if main:
            setattr(self,'data_base',odict['data_base'])
            os.makedirs(odict['tmp_dir'], exist_ok=True)
            os.makedirs(odict['daily_rpt_dir'], exist_ok=True)
            os.makedirs(odict['data_base'], exist_ok=True)
        if opts:
            for key in opts.__dict__:
                setattr(self,key,opts.__dict__[key])
            self.cook_mail_to(opts)
            self.cook_fresh(opts)
        if flavor:
            self.cook_hosts(opts)
        for key,value in self.__dict__.items():
            setattr(__class__,key,value)
        if flavor:
            self.flavor = flavor
        #self.fresh = 0
            __class__.instance = self
    ########################################################################
    def get_SetOpts_obj(flavor):
    ########################################################################
        """Gets the object from class instances[]  where self.flavor= flavor"""

        for obj in __class__.instances:
                if obj.flavor == flavor:
                    return(obj)
        return(None)
    ########################################################################
    def cook_fresh(self,opts=None):
    ########################################################################
         """Cook the fresh value to minutes, default 24 hours."""

         #self.fresh = 0
         if type(self.fresh) == int:
              return
         self.fresh = int(self.__dict__.get('fresh',24))
         self.fresh = self.fresh * 60 * 60
         try:
             if opts.force:
                 self.fresh = 0
         except AttributeError:
             pass
         #print(self.fresh)
    ########################################################################
    def cook_mail_to(self,opts=None):
    ########################################################################
        """\nConfigure the self.email_to attribute to be a list.
            May override from sys.argv --mail"""

        self.log.debug("setup.SetOpts Cooking email settings to a list")
        #print (self.email_to)
        try:
            self.email_to=self.email_to.split(',')
        except AttributeError  :
            pass
        ### --mail cli parameters override the .conf settings
        if hasattr(opts,'mail') and opts.mail:
            self.email_to=[]
            for mail in opts.mail :
                #if '@' in mail and mail.lower() !='none' :
                if  mail.lower() ==  'none':
                    self.email_to = None
                    return
                if  '@' in mail:
                    self.email_to.append(mail)
                else:
                    log.error( "{} not a valid email address".format(mail))
    ########################################################################
    def cook_hosts(self,opts):
    ########################################################################
        """\nCooks the self.hosts list to be operated against. 
 
           Copies self.hosts to self.hostsfile.
           Sets self.hosts = []
           Checks for the hosts file existenace from self.hostsfile attribute.
           Read the file contents from self.hostsfile attribute.
           Sets the self.hosts [] list to be the contents of the read in file from self.hostsfile.
           If the sys.argv(--host) is set, set the hosts list from sys.argv --host instead.
           Required parameters:
                op = object of the argparse() class,  parsed sys argv  setting
        """
      
        self.log.debug("setup.SetOpts Cooking hosts to a list")
        #note, hosts_file is a legacy specification in the .conf files
        #We want hostsfile because pyhthonic attributes should not have _
        try:
            self.hostsfile = self.hosts_file
        except AttributeError:
            self.log.info("Hosts file specification does not exist")
        finally:
            self.hosts = []
        if opts.host:
            self.hosts=opts.host
            return True
        try:
            self.hosts = parse_hosts_file(self.hostsfile)
        except AttributeError:
            self.hosts = []
##########################################################################
def parse_hosts_file(hfile):
##########################################################################
    """Parse the passed in hosts file and return list of hosts."""
    config = configparser.ConfigParser()
    hosts=[]
    log.info("Parsing hosts file %s"  % hfile)
    linere=re.compile(r'^\s*(\S+)\s*(#.+)*$')
    try:
        fh = open(hfile)
        for line in  fh:
            if re.match('^\s*$',line) : continue
            line=line.strip()
            line=line.split('#')[0]
            if line :
                hosts.append(line)
    except FileNotFoundError:
        log.error("%s not found, exiting"  % hfile)
        sys.exit(2)   
    fh.close()
    return hosts

##########################################################################
def parse_config_files(main=None,flavor=None,script=None):
##########################################################################
    """Parse .conf files in ../etc location and return dict{}.

       Takes 3 args:
              main=
              flavor=
              script=
       Each .conf file configs can overwrite the config of a preceeding .conf.

       Files should be specified without the .conf suffix.
       Results go into op{} dictionary which gets return(op).
       <script>.suffix gets stripped when looking for .conf.
       Example: script vnx_foo.py = vnx_foo.conf
       Files parsed in order =
           ../etc/<main>.conf
           ../etc/<flavor>.conf
           ../etc/<script>.conf
    """
    config = configparser.ConfigParser(interpolation=None,strict=False)
    op={}
    for file in (main,flavor,script):
        if not file :
            continue
        file = "%s%s%s" % ('../etc/',file,'.conf')
        log.info("Parsing .conf file %s"  % file)
        try:
            with open(file) as stream:
                lines = itertools.chain(("[top]",),stream)
                try:
                   config.read_file(lines)
                except configparser.DuplicateOptionError as err:
                   log.error(err)
                   print(err)
                   sys.exit(1)
        except FileNotFoundError:
            log.error("%s not found, skipping"  % file)
        igd = {} #Ignore dict
        igcmdre = re.compile('^ignorecmd\d+')
        for section in config :
            for k,v in config[section].items() :
               log.debug("section=>%s key=>%s value=>%s" % (section,k,v))
               if section.lower() == 'top':
               #if section.lower() == 'global':
                   op[k]=v
                   mo = igcmdre.search(k)
                   if mo:
                      #print("IGNORE found = {}".format(v))
                      igd[v.lower()] = True
               else:
                   if section not in op: op[section]={}
                   op[section][k]=v
    for k in igd:
        if k in op:
            del op[k]
    hosts.errors_check('hosts.parse_config_files()')
    return op
###########################################################################
class SimpleArgParse(argparse.ArgumentParser):
    """Add command line args options for argv with some common
       arguments preconfigured. Simple version with fewer options.
    """
    def __init__(self,description="Script description goes here"):
        super().__init__(description=description)
        self.add_std_args()
    def add_std_args(self):
        script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) 
        shortscript = script.split('.')[0] ##script name minus extension
        logdir  = "../var/log"
        logfile = "%s.log" % shortscript
        self.add_argument('-v','--verbose',action='store_true',help='Verbosely display actions.')
        self.add_argument('-V','--version',action='store_true',help='Display version.')
        self.add_argument('--debug',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--debugssh',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--history',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('-m','--mail',default=[],action='append',help='Specify email addresses, multiple -m or seperating with , allowed.')
        self.add_argument('--logdir',default=logdir, help=argparse.SUPPRESS)
        self.add_argument('--logfile',default=logfile, help=argparse.SUPPRESS)
###########################################################################
class ArgParse(argparse.ArgumentParser):
###########################################################################
    """Add command line args options for argv with some common
       arguments preconfigured.
    """
    

    def __init__(self,description="Script description goes here"):
        super().__init__(description=description)
        self.add_std_args()
    def add_std_args(self):
        script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) ##full script name no paths
        shortscript = script.split('.')[0] ##script name minus extension
        logdir  = "../var/log"
        logfile = "%s.log" % shortscript
        self.add_argument('-v','--verbose',action='store_true',help='Verbosely display actions.')
        self.add_argument('-V','--version',action='store_true',help='Display version.')
        self.add_argument('-f','--force',action='store_true',help='Force update the hosts cache for each cmd.')
        self.add_argument('--missing',action='store_true',help='If cmd file is missing from host cache, not not attempt to get the live data')
        self.add_argument('--fresh',default='24',help='Specify the fresh period.')
        self.add_argument('--debug',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--debugssh',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--history',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('-m','--mail',default=[],action='append',help='Specify email addresses, multiple -m or seperating with , allowed.')
        self.add_argument('--logdir',default=logdir, help=argparse.SUPPRESS)
        self.add_argument('--logfile',default=logfile, help=argparse.SUPPRESS)
        self.add_argument('-H','-ho','--host',default=[],action='append',help='Specify host. Multiple --host allowed.')
    ########################################################################
    def parse_args(self):
    ########################################################################
        """Adds some of the needed options for all scripts,
           these options are not actually part of CLI args,
           but added to the object as convenience.
        """

        obj=super().parse_args()
        obj.loglevel = logging.INFO
        if obj.debug:
            obj.verbose = True
            obj.loglevel = logging.DEBUG
        #full script name no paths
        obj.script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) 
        ##script name minus extension
        obj.shortscript = obj.script.split('.')[0]
        obj.scriptbin = os.getcwd()
        ##full script name including path
        obj.fullscript = os.path.join(obj.scriptbin,obj.script) 
        obj.fullscript = os.path.normpath(obj.fullscript)
        obj.fulllogfile = os.path.join(obj.logdir,obj.logfile)
        obj.fulllogfile = os.path.normpath(obj.fulllogfile)
        ### if -m <emai> specified on CLI, we want to ignore 
        ### append from the .conf file
        if obj.mail:
            obj.noappendmail = True
        return obj
###########################################################################
class ArgParseLight(argparse.ArgumentParser):
###########################################################################
    """Add command line args options for argv with some common
       arguments preconfigured. This could use cleanup and code dedupe from
       class ArgParse.
    """
    def __init__(self,description="Script description goes here"):
        super().__init__(description=description)
        self.add_std_args()
    def add_std_args(self):
        script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) ##full script name no paths
        shortscript = script.split('.')[0] ##script name minus extension
        logdir  = "../var/log"
        logfile = "%s.log" % shortscript
        self.add_argument('-v','--verbose',action='store_true',help='Verbosely display actions.')
        self.add_argument('-V','--version',action='store_true',help='Display version.')
        self.add_argument('-f','--force',action='store_true',help='Force update the hosts cache for each cmd.')
        self.add_argument('--missing',action='store_true',help='If cmd file is missing from host cache, not not attempt to get the live data')
        self.add_argument('--fresh',default='24',help='Specify the fresh period.')
        self.add_argument('--debug',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--debugssh',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--history',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--logdir',default=logdir, help=argparse.SUPPRESS)
        self.add_argument('--logfile',default=logfile, help=argparse.SUPPRESS)
    ########################################################################
    def parse_args(self):
    ########################################################################
        """Adds some of the needed options for all scripts,
           these options are not actually part of CLI args,
           but added to the object as convenience.
        """

        obj=super().parse_args()
        obj.loglevel = logging.INFO
        if obj.debug:
            obj.verbose = True
            obj.loglevel = logging.DEBUG
        #full script name no paths
        obj.script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) 
        ##script name minus extension
        obj.shortscript = obj.script.split('.')[0]
        obj.scriptbin = os.getcwd()
        ##full script name including path
        obj.fullscript = os.path.join(obj.scriptbin,obj.script) 
        obj.fullscript = os.path.normpath(obj.fullscript)
        obj.fulllogfile = os.path.join(obj.logdir,obj.logfile)
        obj.fulllogfile = os.path.normpath(obj.fulllogfile)
        obj.host=None
        return obj
###########################################################################
class ArgParseBare(argparse.ArgumentParser):
###########################################################################
    """slimmer version of ArgParseLight
    """
    def __init__(self,description="Script description goes here"):
        super().__init__(description=description)
        self.add_std_args()
    def add_std_args(self):
        script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) ##full script name no paths
        shortscript = script.split('.')[0] ##script name minus extension
        logdir  = "../var/log"
        logfile = "%s.log" % shortscript
        self.add_argument('-v','--verbose',action='store_true',help='Verbosely display actions.')
        self.add_argument('-V','--version',action='store_true',help='Display version.')
        self.add_argument('--debug',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--debugssh',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--history',action='store_true',help=argparse.SUPPRESS)
        self.add_argument('--logdir',default=logdir, help=argparse.SUPPRESS)
        self.add_argument('--logfile',default=logfile, help=argparse.SUPPRESS)
    ########################################################################
    def parse_args(self):
    ########################################################################
        """Adds some of the needed options for all scripts,
           these options are not actually part of CLI args,
           but added to the object as convenience.
        """

        obj=super().parse_args()
        obj.loglevel = logging.INFO
        if obj.debug:
            obj.verbose = True
            obj.loglevel = logging.DEBUG
        #full script name no paths
        obj.script = os.path.basename((os.path.realpath(sys.modules['__main__'].__file__))) 
        ##script name minus extension
        obj.shortscript = obj.script.split('.')[0]
        obj.scriptbin = os.getcwd()
        ##full script name including path
        obj.fullscript = os.path.join(obj.scriptbin,obj.script) 
        obj.fullscript = os.path.normpath(obj.fullscript)
        obj.fulllogfile = os.path.join(obj.logdir,obj.logfile)
        obj.fulllogfile = os.path.normpath(obj.fulllogfile)
        obj.host=None
        return obj
###########################################################################
###########################################################################
class LogAdapter(logging.LoggerAdapter):
###########################################################################
    """\nContext message to logger to add the hostname of the 
       hosts.host object.
    """
        
    def process (self,msg,kwargs):
        return '%s [%s] %s' % (self.extra['flavor'],self.extra['host'], msg), kwargs
    
###########################################################################
def config_logging(opt=None):
###########################################################################
    """Configure logging.

       op=None
           An object of hosts.parse_args, required.
    """    
    log = logging.getLogger()
    log.setLevel(opt.loglevel)
    formatter = logging.Formatter('%(asctime)s %(module)s.%(funcName)s() %(levelname)s - %(message)s',"%Y-%m-%d %H:%M:%S")
    hosts.errio = io.StringIO()
    if hosts.errio:

        eh = logging.StreamHandler(hosts.errio)
        eh.setLevel(logging.ERROR)
        eh.setFormatter(formatter)
        log.addHandler(eh)
    
    try:
        logfh = logging.FileHandler(opt.fulllogfile)
    except FileNotFoundError:
        #print("making log DIR => {}".format(opt.logdir))
        os.makedirs(opt.logdir, exist_ok=True)
        logfh = logging.FileHandler(opt.fulllogfile)
      
    logfh.setFormatter(formatter)
    log.addHandler(logfh)
    if opt.verbose:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        log.addHandler(ch)
    return log
###########################################################################
def errors_alert(opts):
########################################################################### 
    """\nSends email to opts.adminemail if data detected in hosts.errorpt[].
    """
    if not hosts.errorrpt :
        return False
    try:
        getattr(opts,'mail')
    except AttributeError:
        print("ERRORS detected:")
        for line in hosts.errorrpt: print(line)
        return False
    if  'none' in opts.mail:
        log.info("skipping admin email because --mail none")
        return False
    log.info("Forming error rpt email to admin {}".format(opts.adminemail))
    r  = rpt.Rpt(opts=opts)
    r.mailto = opts.adminemail
    r.mailsubject = "Error with script " + opts.email_subject
    r.add_css_heading("Script error messages","Red")
    r.add_css_table(data=hosts.errorrpt,header=["Host","Message"])
    r.send_email()
###########################################################################
def errors_check(host='NA'):
###########################################################################
   """\nChecks the hosts.errors stream for messages..
       
      If hosts.errors contains contains messages, put each message to
      array hosts.errorrpt[] and then delete the contents of hosts.errors.
      Return True  for errors found and False if not errors.
   """
   emessages = hosts.errio.getvalue()
   if emessages:
       log.warn("hosts.errors_check->Errors detected with {}".format(host))
       for line in emessages.split('\n'):
           if not line : continue
           hosts.errorrpt.append([host,line])
       hosts.errio.truncate(0)
       hosts.errio.seek(0)
       return True
   return False

__version__= 0.32
###########################################################################
def history():
###########################################################################
    """0.10 Initial
       0.11 logging
       0.12 streamline .conf parsing
       0.14 emails in errors_alert
       0.15 --missing
       0.16 creation of daily_report_dir
       0.17 set all paths to os.path.normpath
       0.18 fix to self.fresh value changing for every host
       0.19 error_alert now uses rpt.Rpt() for sending email
       0.20 removed --csv option
       0.21 removed def create_paths
       0.22 making dir for logger moved to config_logging
       0.23 IGNORECMD(d+) support in parse_config_files()
       0.24 except configparser.DuplicateOptionError now exits
       0.25 --history added
       0.26 os.umask(0o002)
       0.27 cook_mail_to fixed to specify log.error instead of self.warn
       0.28 config = configparser.ConfigParser(interpolation=None,strict=False)
       0.29 ArgParseLight
       0.30 Fixed SetOpts to only load a hosts file if flavor passed
       0.31 SimpleArgParse
       0.32 ArgParseBare
    """
    pass
