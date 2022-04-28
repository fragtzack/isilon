import sys
import os
import subprocess
import logging
import pprint
import time
import socket
import re
import shlex

log = logging.getLogger()

####################################################################
def fname(fname):
    """Provides a filename to use for writing to a file.

        When opening an excel file from a cifs shares, the file gets locked
        and we are unable to create a new excel file with the same name if
        lock exists. This fname method first tries to delete the file if
        file exists. Primary use case is for Excel, but any file can be
        specified.

        If unable to delete the file (usually because file is locked),
        then change the file name to  fname(1).extension or fname(1) if
        no extension. (1) will keep getting incremented up to (10) until
        filename found that can be used or return None.
    """
    modname = fname
    (fname,ext) = (os.path.splitext(fname))
    for x in range(1,10):
        try:
            log.info("Attempting delete => " + modname)
            os.remove(modname)
            return modname
        except FileNotFoundError:
            return modname
        except PermissionError:
            log.info("Unable to delete {} , adding number to name".format(modname))
            modext  = '(' + str(x) + ')' + ext
            modname = fname + modext
    raise FileNotFoundError
    return None
####################################################################
def run_cmd(cmd=None):
   """Run an OS command, better handling then os_cmd"""
   #lcmd = cmd.split()
   lcmd = shlex.split(cmd)
   proc = subprocess.Popen(lcmd,
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
   )
   stdout, stderr = proc.communicate()
   stdout = stdout.decode('utf-8').splitlines()
   stderr = stderr.decode('utf-8').splitlines()
   return proc.returncode, stdout, stderr
####################################################################
def os_cmd(cmd=None):
    """Run an OS command"""
    #p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    #output, err = p.communicate()
    stdout = subprocess.check_output(cmd , stderr=subprocess.STDOUT)
    return stdout.decode()
###########################################################################
def unixtime():
    """Return the current time in unix style."""
    t = time.localtime()
    tz = time.tzname[t.tm_isdst]
    now = (time.strftime("%a %b %d %H:%M:%S {} %Y".format(tz),time.localtime()))
    return(now)

###########################################################################
def check_server(address, port):
    """Checks a TCP socket, return True for listening port else False."""
    s = socket.socket()
    log.debug("Attempting to connect to {} on port {}".format(address,port))
    try:
        s.connect((address, port))
        log.info("port {} is alive on host {}".format(port,address))
        return True
    except socket.error as err:
        log.error("Connection to port {} on host {} failed: {}".format(port,address,err))
        return False
###########################################################################
def list_to_file(llist=None,lfile=None):
    """Writes a list of lists to a file."""
    
    if not llist or not lfile:
        log.error("List of lists and file name required.")
        return False
    log.info("Writing {}".format(lfile))
    with open(lfile,'w') as fh:
        for line in llist:
            print(line,file=fh)
###########################################################################
def to_gb(num=None,spec=None):
    """\nConvert to gb.
         Takes as input a size and a specifier.
         Where size is an integer or float.
         Where specifier = b,k,kb,m,mb,g,gb,t,tb,p,pb
         Returns as "GB => (size)"
    """
    ##Maybe function gets called with number+specifier next to each other?
    ##Example - 100GB instead of expected 100 GB
    if not spec:
        #m = re.search('(\d+)(\D)',num)
        m = re.search('([\d.]+)(\D+)$',num)
        if m : 
            num = m.group(1)
            spec = m.group(2)
        else:
            spec = 'b'
    try:
        i = float(num)
    except ValueError:
        return(num)
    try:
        if spec.lower() == 'b':
             spec = 'Bytes'
        if spec.lower() == 'k':
             spec = 'KB'
        if spec.lower() == 'm':
             spec = 'MB'
        if spec.lower() == 't':
             spec = 'TB'
        if spec.lower() == 'p':
             spec = 'PB'
    except AttributeError:
        pass
    #print("to_gb",i,spec)
    try:
        if spec == None or spec =='Bytes':
            i = float("{0:.3f}".format(i/1024.0/1024.0/1024.0))
        if spec.upper() == 'KB':
            i = float("{0:.3f}".format(i/1024.0/1024.0))
        if spec.upper() == 'MB':
            i = float("{0:.3f}".format(i/1024.0))
        if spec.upper() == 'TB':
            i = float("{0:.3f}".format(i*1024.0))
        if spec.upper() == 'PB':
            i = float("{0:.3f}".format(i*1024.0*1024.0))
    except AttributeError:
        pass
    if (i).is_integer():
        return(int(i))
    else:
        return(i)

__version__ = 0.18
###########################################################################
def history():
###########################################################################
    """\n0.10 Initial
         0.11 os_cmd()
         0.12 unixtime()
         0.13 check_server()
         0.14 unixtime() now includes day of month
         0.15 list_to_file()
         0.16 to_gb()
         0.17 to_gb() returns original input num if not able to return as int
         0.18 to_gb() accounts for PB
         0.19 added shlex for run_cmd
    """
    pass

