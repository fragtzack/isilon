import logging
import sys
import os
import pprint
import re
import time
import socket
import simplejson as json
import paramiko

import hosts
import copy

#Note there is bug with paramiko in assuming that all text output
#can be decoded using UTf-8. In py3compat.py, I have modifed the
# u() method for python 3 to try decode using ISO-8859-1 if UTF8 fails

class Host():
    """Parent class for attributes and methods for a host.
        Main usage should be from derived host classes.
   
        Attributes:
            name           hostname
            flavor         flavor of host: linux,vnx,netapp,windows,etc
            dbdir          directory location of the host cache files
            op             Object of the hosts.SetOpt
            connect        connection object/status of a host
            connecttype    ssh,rest,etc. Only ssh at this time
            ruser          remote user to connect as for host, 
                           usually determined from hosts.SetOpts() 
                           object remote_user.
        Methods:
            self.remote_cmd(cmd)
                Run remote command on host.
            self.get_ssh_client 
                Gets and returns ssh connection object.
            self.get_file_cmd(getcmd that was specified in 
                one of the .conf files. A non file.conf command
                can be created by :
                    self.command_<cmd file name> = <cmd to be run>
    """

    ##############################################################
    def __init__(self,name=None):
    ##############################################################
       self.logger = logging.getLogger()
       if not name:
           self.logger.error("No hostname defined")
           return None
       self.name=name
       try:
           hosts.SetOpts.instance
       except AttributeError as err:
           self.logger.error(err)
           return None
       self.op=hosts.SetOpts.instance
       mydict = {'host': self.name,'flavor': self.__class__.__name__}
       self.log = hosts.LogAdapter(self.logger, extra=mydict)
       self.retval=None
       self.stdout=[]
       self.stderr=[]
       #pprint.pprint(self.op.__dict__);sys.exit()
       try:
           self.dbdir = "{}/{}".format(self.op.data_base,self.name)
           self.connecttype = self.op.connecttype
           self.ruser = self.op.remote_user
       except AttributeError as err:
           self.logger.error("Unable to set attribute for {}->{}".
                         format(self.name,err))
           ehelp="Probably need to configure in the hosts.conf."
           self.logger.error(ehelp)
           self.logger.debug("name=>{} dbdir=>{}".format(self.name,self.dbdir))
           return False
       try:
           os.makedirs(self.dbdir, exist_ok=True)
       except:
           self.logger.critical("Unable to make directory {}".format(self.dbdir))
           return False
       self.log.info("{} host=>{}".format(self.__class__.__name__,name))
       #return True
    ##############################################################
    def cmd(self,cmd=None):
    ##############################################################
        """Run a remote command on self.name

           Returns stdout,stderr,retval.
           Deletes existing self.retval,self.stdout,self.stderr.
           Sets self.retval,self.stdout,self.stderr.
           self.connecttype determines how to connect.
           For ssh connecttype:
                  If sshclient object does not exist, 
                  call get_ssh_client().
        """
        self.retval=None
        self.stdout=[]
        self.stderr=[]
        self.log.info("Running ssh command=>{}".format(cmd))
        exlist = (EOFError,socket.gaierror,socket.timeout,
                  paramiko.ssh_exception.SSHException)
        try:
            stdin, stdout, stderr = self.connect.exec_command(cmd)
            self.log.debug("First cmd try")
        except AttributeError:
            self.log.debug("AttibuteError, need to connect ssh client")
            try:
                self.get_ssh_client()
            except exlist:
                return False
            except FileNotFoundError:
                return False
            stdin, stdout, stderr = self.connect.exec_command(cmd)
        except EOFError:
            self.log.debug("EOFError, ssh client needs reconnecting") 
            self.connect.close()
            try:
                self.get_ssh_client()
            except exlist:
                return False
            except FileNotFoundError:
                return False
            stdin, stdout, stderr = self.connect.exec_command(cmd)
        #except SSHException:
            #self.log.debug("EOFError, ssh client lost connection") 
            #self.connect.close()
            #try:
                #self.get_ssh_client()
            #except exlist:
                #return False
        except paramiko.ssh_exception.SSHException as err:
            self.log.debug(err)
            self.log.debug("retrying connection")
            self.connect.close()
            try:
                self.get_ssh_client()
            except exlist:
                return False
            stdin, stdout, stderr = self.connect.exec_command(cmd)
        stdin.flush()
        stdin.close()
        for line in stdout:
            self.stdout.append(line.rstrip())
        for line in stderr:
            self.stderr.append(line.rstrip())
        self.retval=stdout.channel.recv_exit_status()
        stdout.flush()
        stderr.flush()
        return(self.stdout)
    ##############################################################
    def get_ssh_client(self):
    ##############################################################
       """Create and return an ssh client object."""

       self.log.info("ssh connect to {}".format(self.name))
       conf = self.op.__dict__['SSH']
       #pprint.pprint(conf)
       logging.getLogger("paramiko").setLevel(logging.WARNING)
       if self.op.debug:
           logging.getLogger("paramiko").setLevel(logging.INFO)
       if self.op.debugssh:
           logging.getLogger("paramiko").setLevel(logging.DEBUG)
       self.connect = paramiko.SSHClient()
       password = None
       idkey = conf.get('identityfile' , None)
       if idkey and not os.path.isfile(idkey):
           self.log.error("{} ssh idkey file does not exist".format(idkey))
           raise FileNotFoundError
           return False
       if hasattr(self,'password'):
           password = self.password
       ##cmmented below because of changing host keys on isilon
       #try:
           #self.connect.load_host_keys(filename = conf['userknownhostsfile'])
       #except FileNotFoundError as err:
           #self.log.error(err)
           #npath = os.path.join(self.op.__dict__['scriptbin'],'../etc/.ssh')
           #nhosts = os.path.join(npath,'known_hosts')
           #open(nhosts,'w').close()
           #self.connect.load_host_keys(filename = nhosts)
       self.connect.set_missing_host_key_policy(paramiko.AutoAddPolicy())
       try:
           self.connect.connect(self.name,
                                #hostkey = None,
                                username = self.ruser,
                                password = password,
                                key_filename = idkey,
                                look_for_keys = False,
                                allow_agent = False,
                                timeout = 15,
                                auth_timeout = 15,
                                )
                                #banner_timeout = 5)
       except paramiko.ssh_exception.SSHException as err:
           self.log.critical(err)
           raise
           return False
       except EOFError as err:
           self.log.critical(err)
           raise
           return False
       except socket.gaierror as err:
           self.log.critical(err)
           raise
           return False
       except socket.timeout as err:
           self.log.critical(err)
           raise
           return False
    ##############################################################
    def get_file_cmd(self,getcmd):
    ##############################################################
       """Gets a cmd result from the host cache "dbdir" system.
          
          Verifies getcmd has been specified as a host 
          object attribute starting with command_ in the 
          class hosts.SetOpts object, aliased as self.op.
          Example valid command attribute for "uptime" getcmd: 
              self.op.command_uptime
          From getcmd, cook 4 strings:
                cmdkey =  getcmd with the  command_ added to start.
                cmdval = The value of the command key.
                gettxtfile  = A file name=>self.db_dir.getcmd."txt".
                getcmdfile  = A file name->self.db_dir.getcmd.cmd.
          After verify and cook, call self.is_file_fresh(getcmd.txt)
          to determine if the getcmd.txt  file is fresh.
          If the file is fresh, 
                  return the contents of the file via self.stdout.
          If the file is not fresh or not exist, calls:
                 self.cmd(getcmd)
                 self.set_dbdir_file(gettxtfile,self.stdout)
                 self.set_dbdir_file(getcmdfile,cmdval)
           returns self.stdout or Exception        
       """
       cmdkey="command_{}".format(getcmd)
       try:
           cmdval=getattr(self.op,cmdkey)
           gettxtfile="{}.txt".format(getcmd)
           getcmdfile="{}.cmd".format(getcmd)
       except AttributeError as err:
           self.log.exception(err)
           return False
       self.log.debug("getcmd found =>{}".format(cmdkey))
       if self.is_file_fresh(gettxtfile):
          fout = self.get_dbdir_file(gettxtfile)
          if fout: 
              self.stdout = fout
              return self.stdout
          elif self.op.missing:
                self.log.info("{}, but --missing set".format(self.stdout))
                return True
       else:
          self.cmd(cmdval)
          if not self.stdout:
              return False
          if self.retval_chk(cmdval):
              return False
 
          self.set_dbdir_file(getcmdfile,cmdval,raw=True)
          self.set_dbdir_file(gettxtfile,self.stdout)
          return self.stdout
    ##############################################################
    def retval_chk(self,getcmd=None):
    ##############################################################
        """\nCheck the recently ran command for retval != 0.
    
           If retval != 0 , log.error with RC and self.stderr.
           If retval != 0 , return True
           If retval == 0 , return False
        """

        if self.retval != 0:
            self.log.error("Non-Zero RetVal=>{} cmd=>{}"
                     .format(self.retval,getcmd))
            if self.stdout:
                self.log.error("STDOUT=>{}".format("\n".join(self.stdout)))
            if self.stderr:
                self.log.error("STDERR=>{}".format("\n".join(self.stderr)))
            return True
        return False
    ##############################################################
    def get_dbdir_file(self,ifile=None,raw=False):
    ##############################################################
        """Get and return contents of a file from dbdir.

           Contents returned will be in an array, with /n removed.
           Raw=True will return raw file contents instead of array.
        """
        ifile = os.path.join(self.dbdir,ifile)
        ifile = os.path.normpath(ifile)

        farray = []
        enclist = ('utf-8','latin-1','utf-16','ascii')
          
        try:
            for e in enclist:
                try:
                    self.log.info("Reading {} as {}".format(ifile,e))
                    with open(ifile, mode='r', encoding=e) as fh:
                        if raw:
                            fraw = fh.read()
                            return fraw
                        farray = fh.read().splitlines()
                        return farray
                    #If got here, the failed to read encoding
                    self.log.error("Failed to read encoding for {}"
                                  .format(ifile))
                except (UnicodeDecodeError,UnicodeError) as err:
                    self.log.info(err)
                    continue
        except FileNotFoundError as err:
            self.log.info(err)
        if not farray:
            return None
        return farray
    ##############################################################
    def set_dbdir_file(self,ifile=None,icontent=None,raw=False):
    ##############################################################
        """Write a ifile from icontent to dbdir.
          
           icontent is expected to be an array. A \n will
           be added to form lines in the file for each array
           element.
           
           if raw=True, then icontent will be written raw.
        """
        ifile = os.path.join(self.dbdir,ifile)
        ifile = os.path.normpath(ifile)

        self.log.info("Writing {}".format(ifile))
        with open(ifile,'w') as fh:
            if raw:
                print(icontent,file=fh)
                return True
            #print(icontent,sep="\n",file=fh)
            #fh.write("{}/n".format(item for item in icontent))
            for line in icontent:
                print(line,file=fh)
    ##############################################################
    def is_file_fresh(self,ifile):
    ##############################################################
       """Looks at the time stamp of file and comapres
          to the default fresh value. Returns true or false.
       """
       ifile = os.path.join(self.dbdir,ifile)
       ifile = os.path.normpath(ifile)
       self.log.debug("Checking for freshness=>{}".format(ifile))
       try:
           statinfo = os.stat(ifile)
           filemtime = int(str(statinfo.st_mtime).split('.')[0])
           currtime = int(str(time.time()).split('.')[0])
           cutofftime = currtime - self.op.fresh
           #cutofftime = currtime 
           if cutofftime >  filemtime:
               self.log.info("{} is stale".format(ifile))
               return False
       except FileNotFoundError as err:
           if self.op.missing:
               self.log.info("{} is missing, but --missing set".format(ifile))
               return True
           self.log.warn(err)
           return False
       self.log.info("{} is fresh".format(ifile))
       return True
    ###########################################################################
    def fresh_json(self,ifile=None):
    ###########################################################################
        """\nCheck for freshness and return a json file contents.
             Files names are passed , .json added to file name.
             Return None if file not fresh or file not found
        """
        ifile = ifile + '.json'
        if self.is_file_fresh(ifile):
            return self.get_json_file(ifile)
        return False
    ###########################################################################
    def put_json_file(self,ifile=None,icontent=None,extension=True):
    ###########################################################################
        """\nPuts a json file from icontent object.

             Adds .json extension unless extention=False
        """
        
        if extension:
           ifile = ifile + '.json'
        if not ifile : 
             self.log.warn("No json name specified to write")
             return False
        if not icontent:
             self.log.warn("No content specified to write json file=>{}".format(ifile))
             return False
        jdump = json.dumps(icontent,indent=4)
        self.set_dbdir_file(ifile=ifile,icontent=jdump,raw=True)
        return True
    ###########################################################################
    def get_json_file(self,ifile=None):
    ###########################################################################
        """\nGet a json file and return as dict.
        """

        jread = self.get_dbdir_file(ifile=ifile,raw=True)
        if not jread : return None
        jread = json.loads(jread)
        self.log.debug(pprint.pformat(jread))
        return jread
    ###########################################################################
    def get_stub(self,fcmd):
    ###########################################################################
        fdata = '_' + fcmd
        try:
            return getattr(self,fdata)
        except AttributeError:
            pass
        jobj = self.fresh_json(fcmd)
        if jobj :
            return(setattr(self, fdata, jobj))
        if not self.get_file_cmd(fcmd):
            self.log.warning("{}() file command issue or file command file missing => {} "
                           .format(sys._getframe().f_back.f_code.co_name,fcmd))
            return None 
        if not self.stdout:
            return None
        return None
###########################################################################
__version__ = 0.23
###########################################################################
def history():
###########################################################################
    """\n0.10 Initial
       0.11 logging
       0.12 --missing
       0.14 set_dbfir_file get_dbdir_file changed to add the path
       0.15 get_json_file
       0.16 fresh_json
       0.17 get_stub
       0.18 create empty known_hosts file if not detected
       0.19 get_dbdir_file => loop to try multiple encodings
       0.20 get_ssh_client raises FileNotFound if key does not exists
               This needs to be adjusted if password feature is added
       0.21 Commented out return trie from Host.__init__
       0.22 Commented out trying host keys
       0.23 elif self.op.missing:
                self.log.info("{}, but --missing set".format(self.stdout))
    """
    pass

