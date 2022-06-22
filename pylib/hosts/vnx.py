import pprint
import sys
import re
import copy
import csv
#import xml.etree.cElementTree as ET
#import xmltodict

from .host import Host

####### GLOBABLS ########
skipre = re.compile(r'^\s*$') #blank lines
dmre = re.compile(r'^(\S+)\s+:\s?$') #dm/vdm
#########################


            
class Vnx(Host):
    """VNX type child class of host class."""

    ####################################################################
    def __init__(self,name):
    ####################################################################
        super().__init__(name)

    ####################################################################
    def cmd(self,cmd=None):
    ####################################################################
        cmd="NAS_DB=/nas {}".format(cmd)
        return(super().cmd(cmd))

    ##############################################################
    def retval_chk(self,getcmd=None):
    ##############################################################
        """\nCheck the recently ran command for retval != 0.

           If retval != 0 , log.error with RC and self.stderr.
           If retval != 0 , return True
           If retval == 0 , return False

           Exeptions for VNX:
              if cmd = server_cifs or server_export or server_viruschk or
                        server_nsdomains and retval = 6 return false
                        because a vdm may be unloaded
              if server_viruschk ALL
                        and retval = 7 return false
                        because standby datamover is not an error
              if server_cifssupport ALL
                        and retval = 7 return false
                        because standby datamover is not an error
                        and a dm might not be running cifs
        """
        p = re.compile('^/nas/bin/server_(viruschk|cifssupport|df)\s*ALL')
        pp = re.compile('^/nas/bin/server_(export|viruschk|nsdomains|cifs)\s*ALL')
        if p.match(getcmd):
            if self.retval == 7:
                return False
        if pp.match(getcmd):
            if self.retval == 6:
                return False
        return super().retval_chk(getcmd)
    #################################################################### 
    @property
    def nas_fs_tree_quotas(self):
    #################################################################### 
        """Returns dict of FS tree quota info. Outer key = fs name.
            {fs} => {path> => {details}
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        fields = "BlockGracePeriod BlockHardLimit BlockSoftLimit BlockUsage Comment FileSystem FileSystemID ID InodeGracePeriod InodeHardLimit InodeSoftLimit InodeUsage IsEventSentForBlockHardLimit IsEventSentForBlockSoftLimit IsEventSentForCheckEnd IsEventSentForCheckStart IsGroupQuotasEnabled IsHardLimitEnforced IsLocal IsUserQuotasEnabled Name Path RWMountpoint RWServers RWServersNumeric RWVDMs RWVDMsNumeric".split()
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        tq = {}
        DefaultBlockGracePeriod = DefaultInodeGracePeriod = 'NoLimit'
        for line in self.stdout:
            #print(line)
            csvreader = csv.DictReader(line.splitlines(),
                                 fieldnames=fields,
                                 dialect = 'unix',
                                 delimiter='=')
            #BlockGracePeriod,BlockHardLimit,BlockSoftLimit,BlockUsage
            for row in csvreader:
                #print(line)
                #print(row)
                FileSystem = row['FileSystem']
                if not FileSystem:
                    FileSystem =  row['BlockGracePeriod']
                    DefaultBlockGracePeriod = row['BlockHardLimit']
                    DefaultInodeGracePeriod = row['BlockSoftLimit']
                    continue
                Path = row['Path']
                #print(FileSystem,Path)
                tq.setdefault(FileSystem, {})[Path] = row
                tq[FileSystem][Path]['DefaultBlockGracePeriod'] = DefaultBlockGracePeriod
                tq[FileSystem][Path]['DefaultInodeGracePeriod'] = DefaultInodeGracePeriod
        setattr(self,fdata,tq)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    @property
    def nas_pool(self):
    #################################################################### 
        """\nReturns dict of NAS pool info. Outer key = pool name.
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        fields = "ID Name AvailableMB UsedMB CapacityMB PotentialMB Desc DiskType IsInUse IsUserDefined IsDynamic IsGreedy StorageIDs".split()
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        pl = {}
        for line in self.stdout:
            #print(line)
            csvreader = csv.DictReader(line.splitlines(),
                                 fieldnames=fields,
                                 dialect = 'unix',
                                 delimiter='=')
            for row in csvreader:
                Name = row['Name']
                pl[Name] = row
        setattr(self,fdata,pl)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    @property
    def nas_fs(self):
    #################################################################### 
        """\nTransforms q_nas_fs dict to have the key be fs names 
             instead of ID.
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        qf = {}
        try:
            for id,row in self.q_nas_fs.items():
                qf[row['Name']] = row
            setattr(self,fdata,qf)
        except AttributeError as err:
            self.log.error(err)
            return None
        return  getattr(self, fdata)
    #################################################################### 
    @property
    def dart5_q_nas_fs(self):
    #################################################################### 
        """\nGet dart 5 file system info. 
             Returns a dict with fs id as outer key.
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        fields = ("ID Name Type InodeCount Size Capacity Available Used PercentUsed Blocks StoragePoolId StoragePoolName RWServersNumeric RWVDMsNumeric ROServersNumeric ROVDMsNumeric RWMountPoint AutoExtend MaxSize HWMNumber IsInUse IsUSerQuotasEnabled IsGroupQuotasEnabled IsHardLimitEnforced HasiSCSILun BackupOf Isroot VirtuallyProvisioned CkptSavVolUsedMB ReplicationSessions IsVdmRootFs".split())
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        fsd  = {}
        for line in self.stdout:
            #print(line)
            csvreader = csv.DictReader(line.splitlines(),
                                 fieldnames=fields,
                                 dialect = 'unix',
                                 delimiter=',')
            for row in csvreader:
                ID = row['ID']
                fsd[ID] = row
                fsd[ID]['IsVdmRootFs'] = 'NA' ##to be compat with q_nas_fs
                #if ID != '96':
                    #continue
                #pprint.pprint(row)
                #print(line)
        setattr(self,fdata,fsd)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    @property
    def q_nas_fs(self):
    #################################################################### 
        """\nGet file system info. Returns a dict with fs id as outer key.
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        if self.nas_version.startswith('5'):
            self.log.info("Dart 5 detected")
            setattr(self,fdata,self.dart5_q_nas_fs)
            return  getattr(self, fdata)
        fields = ("ID Name Type InodeCount Size Capacity Available Used PercentUsed Blocks StoragePoolId StoragePoolName RWServersNumeric RWVDMsNumeric ROServersNumeric ROVDMsNumeric RWMountPoint AutoExtend MaxSize CkptSavVolUsedMB CkptPctUsed HWMNumber IsInUse IsUSerQuotasEnabled IsGroupQuotasEnabled IsHardLimitEnforced HasiSCSILun HasRepIntCkpts VirtuallyProvisioned BackupOf IsRoot IsVdmRootFs ReplicationSessions".split())
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        fsd  = {}
        for line in self.stdout:
            #print(line)
            csvreader = csv.DictReader(line.splitlines(),
                                 fieldnames=fields,
                                 dialect = 'unix',
                                 delimiter=',')
            for row in csvreader:
                ID = row['ID']
                fsd[ID] = row
                #if ID != '96':
                    #continue
                #pprint.pprint(row)
                #print(line)
        setattr(self,fdata,fsd)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    @property
    def skel(self):
    #################################################################### 
        """\nSkeleton function, copy this for creating new functions
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        #headersname =  'headers_' + fcmd
        #headers = []
        #setattr(self, headersname, [])
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        mts = {}
        for line in self.stdout:
            print(line)
        ### NEW CODE HERE""
        ### Change mts dict name as needed
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    def nas_xml(self):
    #################################################################### 
        print("under construction")
        sys.exit()
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd  
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        #self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        try:
            getattr(self,fdata)
        except AttributeError:
            setattr(self,fdata,{})
        if not self.stdout:
            return None
        tree = ET.ElementTree(ET.fromstring('\n'.join(self.stdout)))
        root = tree.getroot()
        print("HERE")
        print(root.attrib)
        for child in root:
            print(child.tag, child.attrib)
            #print(child.tag.attrib)
        sys.exit()



         ###OLD TO DICT BELOW HERE"
        mydoc = xmltodict.parse('\n'.join(self.stdout))
        setattr(self,fdata,mydoc)
        try:
            self.put_json_file(ifile=fcmd,icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)
        #pprint.pprint( mydoc['CELERRA']['CELERRA_CABINET']['DATA_MOVERS']['MOVER'][2]['@NAME'])
        #pprint.pprint( mydoc['CELERRA']['CELERRA_CABINET']['DATA_MOVERS']['MOVER'][1]['LOGICAL_DEVICES'])
        #pprint.pprint(mydoc['CELERRA']['CELERRA_CABINET'])
        #sys.exit()
        for dm in mydoc['CELERRA']['CELERRA_CABINET']['DATA_MOVERS']['MOVER']:
            #print(dm)
            #pprint.pprint(dm['@NAME'])
            dmname = (dm['@NAME'])
        tree = ET.ElementTree(ET.fromstring('\n'.join(self.stdout)))
        root = tree.getroot()
        #print(root.tag,root.attrib)
        #pprint.pprint(tree)
        for dm in tree.iter('DATA_MOVERS'):
            print(dm.get('NAME'))
            for child in dm:
                name = child.get('NAME')
                print(name)
                #print(child.tag, child.attrib )
        #for child in root:
            #print(child.tag, child.attrib)
        
        #for line in self.stdout:
            #setattr(self,fdata,line)
        #return getattr(self,fdata)
    #################################################################### 
    @property
    def dart5_nas_server_ifconfig(self):
    ####################################################################
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        #"Name Address Broadcast Device DeviceType EthernetAddress \
        self.nas_server_ifconfig_header = \
              "Name Address Broadcast Device \
               ID MTU Protocol Status Subnet VLAN".split()
        fields= self.nas_server_ifconfig_header
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        #self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        try:
            getattr(self,fdata)
        except AttributeError:
            setattr(self,fdata,{})
        #fieldnames = "Name Address Broadcast Device DeviceType EthernetAddress ID MTU Protocol Status Subnet VLAN".split()
        srvre = re.compile(r'^(\S+) :\s?$') #datamover
        skipre = re.compile(r'^\s*$') #blank lines
        for line in self.stdout:
            #if (re.match('^(\s+)?\r?\n', line)): #skip blank lines
            #print(line)
            if skipre.search(line):
                continue
            mo = srvre.search(line)
            if mo:
                 srv = mo.group(1)
                 continue
            self._dart5_nas_server_ifconfig.setdefault(srv,{})
            self.log.debug("Processing dm=>{}".format(srv))
            csvreader = csv.DictReader(line.splitlines(),
                                 fieldnames=fields,
                                 dialect = 'unix',
                                 delimiter='=')
            for row in csvreader:
                name = row['Name']
                self._dart5_nas_server_ifconfig[srv][name] = row
                self._dart5_nas_server_ifconfig[srv][name]['DeviceType'] = ' '
                self._dart5_nas_server_ifconfig[srv][name]['EthernetAddress'] = ' '
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)

    #################################################################### 
    @property
    def nas_server_ifconfig(self):
    ####################################################################
        """\nGets COMMAND nas_server_ifconfig and returns data in dict.
           The dict keys =
                  {dm} -> {interface_name} -> {inner dict}
        """

        if self.nas_version.startswith('5'):
            self.log.info("Dart 5 detected")
            return self.dart5_nas_server_ifconfig
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        self.nas_server_ifconfig_header = \
              "Name Address Broadcast Device DeviceType EthernetAddress \
               ID MTU Protocol Status Subnet VLAN".split()
        fields = self.nas_server_ifconfig_header
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        #self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        try:
            getattr(self,fdata)
        except AttributeError:
            setattr(self,fdata,{})
        #fieldnames = "Name Address Broadcast Device DeviceType EthernetAddress ID MTU Protocol Status Subnet VLAN".split()
        srvre = re.compile(r'^(\S+) :\s?$') #datamover
        skipre = re.compile(r'^\s*$') #blank lines
        for line in self.stdout:
            #if (re.match('^(\s+)?\r?\n', line)): #skip blank lines
            #print(line)
            if skipre.search(line):
                continue
            mo = srvre.search(line)
            if mo:
                 srv = mo.group(1)
                 continue
            self._nas_server_ifconfig.setdefault(srv,{})
            self.log.debug("Processing dm=>{}".format(srv))
            csvreader = csv.DictReader(line.splitlines(),
                                 dialect = 'unix',
                                 delimiter='=')
            for row in csvreader:
                name = row['Name']
                self._nas_server_ifconfig[srv][name] = row
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)
    #################################################################### 
    @property
    def server_nsdomains(self):
    ####################################################################
        if self.nas_version.startswith('5'):
            self.log.info("Dart 5 detected,nsdomains not supported")
            return None
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        skipre = re.compile(r'^\s*$') #blank lines
        srvre = re.compile(r'^(\S+)\s+:\s?$')
        enabledre = re.compile(r'^NSDOMAINS\s+CONFIGURATION\s+=\s+Enabled')
        colonre = re.compile(r'^(\S.+\S)\s+:\s+(\S.+)')
        dm = ""
        ns = {}
        for line in self.stdout:
            #print(line)
            if skipre.search(line):
                continue
            mo = srvre.search(line)
            if mo:
                dm = mo.group(1)
                enabled = False
                #print(dm)
                continue
            mo = enabledre.search(line)
            if mo:
                ns[dm] = {}
                continue
            mo = colonre.search(line)
            if mo:
                key = mo.group(1)
                ##Error 4019 because an unloaded VDM
                if re.search('^Error 4019:',key):
                    continue
                val = mo.group(2)
                ns[dm][key] = val
        setattr(self,fdata,ns)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)
    #################################################################### 
    @property
    def server_mount_fs(self):
    #################################################################### 
        """\nReturns a dict of dict of dicts. 
             With the outer key = dm/vdm inner key FS.
             {dm}-> {fs} -> {mount,fs,options,etc}
             This info is a transformed view of sub server_mount dict
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        if not self.server_mount:
            return False
        setattr(self,'_server_mount_fs',{})
        for srv in self._server_mount:
            for mnt,inner in self._server_mount[srv].items():
                #print(srv,mnt) ; pprint.pprint(inner)
                fs = inner['fs']
                self._server_mount_fs[fs] = inner
                self._server_mount_fs[fs]['mount'] = mnt
                self._server_mount_fs[fs]['server'] = srv
        try:
            return  getattr(self, fdata)
        except AttributeError:
            None
    #################################################################### 
    @property
    def server_mount(self):
    #################################################################### 
        """\nReturns a dict of dict of dicts. 
             With the outer key = dm/vdm inner key mounts.
             {dm}-> {mount} -> {fs,options,etc}
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        dm = ""
        mts = {} ##dict being returned
        mntre = re.compile(r'^(.+?)\son\s(\S+)\s(.+)')
        accessre = re.compile(r'accesspolicy=(\w+)')
        rwrore = re.compile(r',(ro|rw)(,|$)')
        for line in self.stdout:
            #print(line)
            if skipre.search(line):
                continue
            mo = dmre.search(line)
            if mo:
                dm = mo.group(1)
                mts[dm] = {}
                continue
            mo = mntre.search(line)
            if mo:
                fs = mo.group(1)
                mnt = mo.group(2)
                options = mo.group(3)
                self.log.debug("fs=>{} mnt=>{} options=>{}"
                              .format(fs,mnt,options))
                accesspolicy = None
                mo = accessre.search(options)
                if mo:
                    accesspolicy = mo.group(1)
                rwro = None
                mo = rwrore.search(options)
                if mo:
                    rwro = mo.group(1)
                #print("dm=>{} fs=>{} mnt=>{} options=>{} policy=>{} rwro=>{}"
                     #.format(dm,fs,mnt,options,accesspolicy,rwro))
                mts[dm][mnt] = {}
                mts[dm][mnt]['options'] = options
                mts[dm][mnt]['rwro'] = rwro
                mts[dm][mnt]['accesspolicy'] = accesspolicy
                mts[dm][mnt]['fs'] = fs
        #pprint.pprint(mts)
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def server_export(self):
    ####################################################################
        """\nReturns a dict with the outer key = dm/vdm inner key paths.
             Inside the path key is list of shares/exports.
             {dm} -> {path} -> [share1,share2,share3,..]
             Each share is another dict
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        se = {} ## server_export dict to build
        sharere = re.compile(r'^share\s+"(.+?)"\s+"(.+)"\s(.*)')
        exportre = re.compile(r'^export\s"(.+?)"\s(.*)')
        dm = ""
        exp = {} ##dict  being returned
        for line in self.stdout:
            #print(line)
            if skipre.search(line):
                continue
            mo = dmre.search(line)
            if mo:
                dm = mo.group(1)
                exp[dm] = {}
                #print("DM",dm)
                continue
            #if dm != 'somnas500vfs06_vdm':
                 #continue
            #print(line)
            mo = sharere.search(line)
            if mo:
                #print(line)
                path = mo.group(2)
                sdict = parse_share(mo.group(1),path,mo.group(3))
                #pprint.pprint(sdict)
                exp[dm].setdefault(path,[]).append(sdict)
            mo = exportre.search(line)
            if mo:
                #print(line)
                path = mo.group(1)
                edict = parse_export(path,mo.group(2))
                #pprint.pprint(edict)
                exp[dm].setdefault(path,[]).append(edict)
            
        setattr(self,fdata,exp)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    #################################################################### 
    @property
    def nas_version(self):
    ####################################################################
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        for line in self.stdout:
            setattr(self,fdata,line)
        return getattr(self,fdata)
    ####################################################################
    @property
    def dart5_q_server_cifs(self):
    ####################################################################
        """\nGets COMMAND dart5_q_cifs_server and returns data in dict.
           Note there can be multiple CIFs server per dm/vdm.
           Therefore the dict keys = 
                  {dm/vdm} -> {Netbios} -> {inner dict}

        """

        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return None
        if not self.stdout:
            return None
        fieldnames = ["Netbios","Realm","Authentication","CifsDomain","Comment","Compname","Computername","HasJoinedDomain","IsDefault","Aliases","ShareHome"]
        srvre = re.compile(r'^(\S+) :\s?$')
        homere = re.compile(r'^ShareHome = (\S+)$')
        skipre = re.compile(r'^,')
        for line in self.stdout:
            if skipre.search(line):
                continue
            mo = srvre.search(line)
            if mo:
                 srv = mo.group(1)
                 continue
            mo = homere.search(line)
            if mo:
                 sharehome = mo.group(1)
                 continue
            try:
                getattr(self,fdata)
            except AttributeError:
                setattr(self,fdata,{})
            self._dart5_q_server_cifs.setdefault(srv,{})
            csvreader = csv.DictReader(line.splitlines(), 
                                 fieldnames=fieldnames, 
                                 dialect = 'unix',
                                 delimiter=',')
            for row in csvreader:
                netbios = row['Netbios']
                self._dart5_q_server_cifs[srv][netbios] = row
                self._dart5_q_server_cifs[srv][netbios]['ShareHome'] = sharehome
        return getattr(self,fdata)
    ####################################################################
    @property
    def q_server_cifs(self):
    ####################################################################
        """\nGets COMMAND q_cifs_server and returns data in dict.
           Note there can be multiple CIFs server per dm/vdm.
           Therefore the dict keys = 
                  {dm/vdm} -> {Netbios} -> {inner dict}
        """

        if self.nas_version.startswith('5'):
            self.log.info("Dart 5 detected")
            return self.dart5_q_server_cifs
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return None
        fieldnames = ["Netbios","Realm","Authentication","CifsDomain","CifsInterfaceAddresses","CifsType","Comment","Compname","Computername","HasJoinedDomain","IsDefault","LocalUsersEnabled","Aliases","ShareHome"]
        srvre = re.compile(r'^(\S+) :\s?$')
        homere = re.compile(r'^ShareHome = (\S+)$')
        skipre = re.compile(r'^,')
        for line in self.stdout:
            if skipre.search(line):
                continue
            mo = srvre.search(line)
            if mo:
                 srv = mo.group(1)
                 continue
            mo = homere.search(line)
            if mo:
                 sharehome = mo.group(1)
                 continue
            try:
                getattr(self,fdata)
            except AttributeError:
                setattr(self,fdata,{})
            self._q_server_cifs.setdefault(srv,{})
            csvreader = csv.DictReader(line.splitlines(), 
                                 fieldnames=fieldnames, 
                                 dialect = 'unix',
                                 delimiter=',')
            for row in csvreader:
                netbios = row['Netbios']
                self._q_server_cifs[srv][netbios] = row
                self._q_server_cifs[srv][netbios]['ShareHome'] = sharehome
        try:
            self.put_json_file(ifile= fcmd ,
                              icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)
    #################################################################### 
    def cook_nas_server(self):
    ####################################################################
        """\nFrom self.nas_server data , creates different mapped view.
             Creates two dicts:

           self._nas_server_type = {} 
                view with type as outer key and name as inner key 
           self._nas_server_type_id =  {}
                view with type as outer key and id as inner key 
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        if not self.nas_server:
            return False
        setattr(self,'_nas_server_type',{})
        setattr(self,'_nas_server_type_id',{})
        for srv in self._nas_server:
            id = self._nas_server[srv]['Id']
            type = self._nas_server[srv]['Type']
            name = self.nas_server[srv]['Name']
            self._nas_server_type.setdefault(type,{})
            self._nas_server_type_id.setdefault(type,{})
            self._nas_server_type[type][name] = self._nas_server[srv]
            self._nas_server_type_id[type][id] = self._nas_server[srv]
        return True
    #################################################################### 
    @property
    def nas_server_type(self):
    #################################################################### 
        """\nFrom self.nas_server data , provide the 
                view with type as outer key and name as inner key 
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        self.cook_nas_server()
        try:
            return  getattr(self, fdata)
        except AttributeError:
            None
    #################################################################### 
    @property
    def nas_server_type_id(self):
    #################################################################### 
        """\nFrom self.nas_server data , provide the 
                view with type as outer key and id as inner key 
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        self.cook_nas_server()
        try:
            return  getattr(self, fdata)
        except AttributeError:
            None
    ####################################################################
    @property
    def nas_server(self):
    ####################################################################
        """\nGets COMMAND nas_server and returns dict

           self.nas_server = 
                view with server name as outer key
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return None
        if not self.stdout:
            return None
        fieldnames = ["Type","Id","Name","CPU","CPUSpeed","CpuUsage","CifsComment","CifsUserMapperPrimary","Version","StandbyPolicy","StandbyServer","DefaultWINS","Dialect","HasDNS","HasNIS","IsConnected","IsFaulted","IsInUse","IsStandby","MemoryUsage","Model","MotherBoard","PhysicalHost","ShareHome","Slot","CifsUnusedInterfaces","CifsEnabledInterfaces","CifsUsedInterfaces","DNSAddresses","DNSDomain","Dialect","Netbios","NtpServers","PhysicalHost","State","Status","TimeZone","Uptime","CifsDomain","Compname"]
        csvdump = csv.DictReader(self.stdout, 
                                 fieldnames=fieldnames, 
                                 delimiter='=',
                                 quotechar='"')
        setattr(self,fdata,{})
        setattr(self,'_nas_server_type',{})
        setattr(self,'_nas_server_type_id',{})
        for row in csvdump:
            name = row['Name']
            type = row['Type']
            id = row['Id']
            for field in fieldnames:
                #print("{} => {}".format(field,row[field]))
                self._nas_server.setdefault(name,{})[field] = row[field]
                self._nas_server_type.setdefault(type,{})[name]= row[field]
                self._nas_server_type_id.setdefault(type,{})[id]= row[field]
        try:
            self.put_json_file(ifile= fcmd ,
                              icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return getattr(self,fdata)
    ####################################################################
    @property
    def homedirs(self):
    ####################################################################
        """\nGets dm/vdm/.etc/homedir for a vnx and then
             returns a dict of dicts.

             Outer dict key = Homedir (complete path to home dir file)
             Inner dict key = Line number from inside file
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        try:
            return getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error")
            return None
        jobj = self.fresh_json(fcmd)
        if jobj :
            return(setattr(self, fdata, jobj))
        self.get_file_cmd('find_dm_homedirs')
        dmout = self.stdout
        self.get_file_cmd('find_vdm_homedirs')
        bothout = dmout + self.stdout
        if not bothout:
             return None
        setattr(self,fdata,{})
        homere = re.compile(r'^homedir=(.+)',re.IGNORECASE)
        setre = re.compile(r'^(([^:]+):?([^:]+):?([^:]+))?',re.IGNORECASE)
        umaskre = re.compile(r'.+:(\d+)$')
        for line in bothout:
           if len(line.strip()) == 0 :
               continue
           mo = homere.search(line)
           if mo:
                homedir=mo.group(1)
                self._homedirs[homedir] = {}
                idx = 1
                continue
           ##Set defaults
           domain = None
           user = None
           path = None
           create = False
           ro = False
           umask = '022'  ## default if umask not specified
           mo = setre.search(line)
           if mo:
                domain = mo.group(2)
                user = mo.group(3)
                path = mo.group(4)
           else:
                self.log.warning("Not a homedir valid line =>" + line)
                continue
           if ':ro:' in line :
                ro = True
           if ':create:' in line :
                create = True
           mo = umaskre.search(line)
           if mo:
                umask = mo.group(1)
           if user and path:
               self.homedirs[homedir][idx]={ 
                                    'domain' : domain ,
                                    'user'   : user   ,
                                    'path'   : path   ,
                                    'create' : create ,
                                    'ro'     : ro     ,
                                    'umask'  : umask 
                                    }
               user = None
               path = None
           else:
               self.log.warning("Not a homedir valid line =>" + line)
               continue          
           idx = idx + 1
        try:
            self.put_json_file(ifile= fcmd ,
                              icontent=getattr(self,fdata))
        except AttributeError:
            return None
        try: 
            return getattr(self,fdata)
        except AttributeError:
            return None
    ####################################################################
    @property
    def lshomedirs(self):
    ###########################################################################
        """\nPerform an ls on all the path locations from vnx.homedirs.

        Record the output in the host cache ../var/db/<host>
        as file name 'homedirs.ls_path.txt' 
        Example file name for /nas/quota/slot_2/root_vdm_2/homefs/users1/path1 ->
          homedir.homefs.users1-SEP-path1
        Copies the dict of dicts self.homedirs and creates a new inner key
         called 'ls' for each line number from inside the homedir file.
         The 'ls' key point to an array of the files/returns from ls command.
        Example dict:
            '/nas/rootfs/slot_2/root_vdm_2/.etc/homedir': {1: {'create': True,
                                                    'domain': '*',
                                                    'path': '/hometest',
                                                    'ro': True,
                                                    'umask': '720',
                                                    'user': '*',
                                                    'ls' : [] }



        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' +  sys._getframe().f_code.co_name
        try:
            return getattr(self, fdata)
        except AttributeError:
            pass
        jobj = self.fresh_json(fcmd)
        self._lshomedirs = {}
        if jobj:
           return(setattr(self, fdata, jobj))
        basere = re.compile(r'^(.+)/.etc/')
        for homedirfile,inner in self.homedirs.items():
            if not homedirfile in self._lshomedirs:
                 self._lshomedirs[homedirfile] = {}
            base = homedirfile.split("/.etc/")[0]
            for idx,linedict in inner.items():
                 linepath = linedict['path']
                 fullpath = "{}{}".format(base,linepath)
                 mycmd = "/bin/ls {}".format(fullpath)
                 self.cmd(mycmd)
                 if self.retval_chk(mycmd) :
                      continue 
                 if not self.stdout :
                      continue 
                 if 'lost+found' in self.stdout: 
                     self.stdout.remove('lost+found')
                 if not homedirfile in self._lshomedirs:
                     self._lshomedirs[homedirfile] = {}
                 self._lshomedirs[homedirfile][idx] = self.homedirs[homedirfile][idx]
                 self._lshomedirs[homedirfile][idx]['ls']=self.stdout
                 self._lshomedirs[homedirfile][idx]['fullpath'] = fullpath
        try:
            self.put_json_file(ifile=fcmd
                                 ,icontent=getattr(self,fdata))
        except AttributeError: 
            return None
        return getattr(self,fdata)
#################################################################### 
##### global regex compiles here for parse_share and parse_export##
#################################################################### 
netbiosre = re.compile('netbios=(\S+)')
typere = re.compile('type=(\S+)')
commentre = re.compile(r'comment="(.+?)"')
rwre = re.compile('rw=(\S+)')
umaskre = re.compile('umask=(\S+)')
maxusrre = re.compile('maxusr=(\S+)')
namere = re.compile('name="(.+?)"')
rwre = re.compile('rw=(\S+)')
rore = re.compile('ro=(\S+)')
rootre = re.compile('root=(\S+)')
accessre = re.compile('access=(\S+)')
anonre = re.compile('anon=(\d+)')
secre = re.compile('sec=(\S+)')
nfs4re = re.compile('\snfsvfsonly\s')
#################################################################### 
def parse_export(path=None,rest=None):
#################################################################### 
    """\nParse a single ^export line from server_export output.
         Returns a dict.
    """
    #print(path,"##",rest)
    d = {}
    d['protocol'] = 'NFS'
    d['path'] = path
    d['name'] = None
    d['sec'] = None
    d['rw'] = None
    d['ro'] = None
    d['root'] = None
    d['access'] = None
    d['comment'] = None
    d['anon'] = None
    d['nfsv4only'] = None
    mo = nfs4re.search(rest)
    if mo:
        d['nfsv4only'] = 'Yes'
    mo = secre.search(rest)
    if mo:
        d['sec'] = mo.group(1)
    mo = namere.search(rest)
    if mo:
        d['name'] = mo.group(1)
    mo = rwre.search(rest)
    if mo:
        d['rw'] = mo.group(1)
    mo = rore.search(rest)
    if mo:
        d['ro'] = mo.group(1)
    mo = rootre.search(rest)
    if mo:
        d['root'] = mo.group(1)
    mo = accessre.search(rest)
    if mo:
        d['access'] = mo.group(1)
    mo = commentre.search(rest)
    if mo:
        d['comment'] = mo.group(1)
    mo = anonre.search(rest)
    if mo:
        d['anon'] = mo.group(1)
    return(d)
#################################################################### 
def parse_share(share=None,path=None,rest=None):
#################################################################### 
    """\nParse a single ^share line from server_export output.
         Returns a dict.
    """
    #print(share,"##",path,"##",rest)
    d = {}
    d['protocol'] = 'CIFS'
    d['share'] = share
    d['path'] = path
    d['netbios'] = None
    d['type'] = None
    d['comment'] = None
    d['rw'] = None
    d['umask'] = None
    d['maxusr'] = None

    mo = netbiosre.search(rest)
    if mo:
        d['netbios'] = mo.group(1)
    mo = typere.search(rest)
    if mo:
        d['type'] = mo.group(1)
    mo = commentre.search(rest)
    if mo:
        d['comment'] = mo.group(1)
    mo = rwre.search(rest)
    if mo:
        d['rw'] = mo.group(1)
    mo = umaskre.search(rest)
    if mo:
        d['umask'] = mo.group(1)
    mo = maxusrre.search(rest)
    if mo:
        d['maxusr' ]= mo.group(1)
    return(d)

__version__ = 0.71
###########################################################################
def history():
###########################################################################
    """0.10 Initial
       0.11 get_homedirs
       0.12 ls_homedirs
       0.14 fresh_json in ls_homedirs
       0.15 get_nas_server and get_nas_server_type
       0.16 q_server_cifs
       0.17 @property
       0.18 nas_version
       0.19 dart5_q_server_cifs
       0.20 nas_server_ifconfig
       0.21 server_nsdomains
       0.30 server_export
       0.32 parse_export() and parse_share()
       0.40 q_nas_fs
       0.50 nas_pool
       0.60 nas_fs_tree_quotas
       0.61 server_export fix for sec= and nfsv4only
       0.62 ret_val_chk accounts for return code 7 for server_df
       0.63 cmd() now wraps super inside return()
       0.70 sub server_mount_fs()
       0.71 fix for nas_fs_tree_quotas not capturing default block/inode
    """
    pass

