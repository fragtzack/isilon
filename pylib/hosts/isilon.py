import pprint
import sys
import re
import copy
import csv
import simplejson as json

from .host import Host
import util
####################################################################
## GLOBALS
colonre = re.compile(r'^\s*(\S[^:]*\S)\s*:{1}?\s*((.*)\s*)')
snamere = re.compile('^\s+Share Name\s*:\s*(.+)\s*')
####################################################################
def gHeaders(hKeys = None , atFront = None ):
   """Generic Headers.
      Given a list(dict.keys()) as hKeys, returns the keys as a sorted list.
      If atFront (list) is passed also, atFront keys are placed at  
      front of list superseding  the sorted order. Pointless without 
      atFront[] passwd.
   """
   headers = []
   headers.extend(atFront)
   for item in sorted(list(hKeys)):
      if item not in atFront:
         headers.append(item)
   return(headers)
####################################################################
class Isilon(Host):
    """Isilon type child class of host class."""

    ################################################################
    def __init__(self,name):
        super().__init__(name)
    ################################################################
    def retval_chk(self,getcmd=None):
        """Check the recently ran command for retval != 0.

           If retval != 0 , log.error with RC and self.stderr.
           If retval != 0 , return True
           If retval == 0 , return False

           Exceptions for isilon:

           if cmd = 'isi_for_array -s isi_radish -q' and retval = 1
           because a node might have been removed.
        """

        p = re.compile('^isi_for_array -s isi_radish -q')
        if p.match(getcmd):
            if self.retval == 1:
                return False
    ####################################################################
    @property
    def skel(self):
        """Skeleton function, copy this for creating new functions
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
    @property
    def cloud_pools(self):
        """Get isi cloud pools and place into dictionary of dictionaries
           where the outer key is the ID.
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
        idre = re.compile('^\s+ID:\s(.+)')
        for line in self.stdout:
            #print(line)
            mo = idre.match(line)
            if mo:
                cid = mo.group(1)
                mts[cid] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1)
                v = mo.group(2)
                mts[cid][k] = v
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def headers_event_group_list(self):
        """Take the keys from json and return a list of unique keys.
           Used for table headers.
           Headers will be sorted, with a few specified at start.
        """
        try:
           group_list = self.event_group_list
        except Exception as err:
            self.log.error("Unexpected error {}".format(err))
            return False
        atFront = ['ID','Causes Long','Lnn']
        oneGroupName = str(list(group_list.values())[0]['ID'])
        oneGroupKeys = group_list[oneGroupName].keys()
        return (gHeaders(oneGroupKeys,atFront))
    ####################################################################
    @property
    def event_group_list(self):
        """Get isi event group list resolved false, into dict.
           outer key is the ID.
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
        mts = {}
        idre = re.compile('^\s+ID:\s(.+)')
        for line in self.stdout:
            #print(line)
            mo = idre.match(line)
            if mo:
                cid = mo.group(1)
                mts[cid] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1)
                v = mo.group(2)
                mts[cid][k] = v
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def cloud_accounts(self):
        """Get isi cloud accounts and place into dictionary of dictionaries
           where the outer key is the ID.
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
        idre = re.compile('^\s+ID:\s(.+)')
        for line in self.stdout:
            #print(line)
            mo = idre.match(line)
            if mo:
                cid = mo.group(1)
                mts[cid] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1)
                v = mo.group(2)
                mts[cid][k] = v
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def filepool_policies(self):
        """Return a dict of filepool policies, outer key = Name
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
        s = {}
        namere = re.compile('^\s+Name:\s(.+)')
        for line in self.stdout:
            #print(line)
            mo = namere.match(line)
            if mo:
                name = mo.group(1)
                s[name] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1).replace(' ','_')
                v = mo.group(2)
                s[name][k] = v

        setattr(self,fdata,s)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent = getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def snapshot_sched_list(self):
        """Return a dict of snapshot schedules, outer key = ID
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
        s = {}
        idre = re.compile('^\s+ID:\s(\S+)')
        for line in self.stdout:
            #print(line)
            mo = idre.match(line)
            if mo:
                id = mo.group(1)
                s[id] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1)
                v = mo.group(2)
                s[id][k] = v

        setattr(self,fdata,s)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def snapshot_snapshots_list(self):
        """Return a dict of snapshots, outer key = ID
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
        s = {}
        idre = re.compile('^\s+ID:\s(\S+)')
        for line in self.stdout:
            #print(line)
            mo = idre.match(line)
            if mo:
                id = mo.group(1)
                s[id] = {}
            mo = colonre.match(line)
            if mo:
                k = mo.group(1)
                v = mo.group(2)
                s[id][k] = v

        setattr(self,fdata,s)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def version(self):
        """Get isi version (OneFS)
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
        self.get_file_cmd(fcmd)
        if not self.stdout:
            return None
        mts = None
        for line in self.stdout:
            #print(line)
            m = re.search(r'OneFS\s+(\S+)\s+',line)
            if m:
                #print(m.group(1))
                mts = m.group(1)
        setattr(self,fdata,mts)
        return(getattr(self, fdata))
    ####################################################################
    @property
    def hw_status(self):
    ####################################################################
        """Return a dict of "isi_for_array isi_hw_status"
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
        prwre = re.compile(r'^\S+-(\d+):\s+PwrSupl:\s+(\S+)\s(.+)')
        hwre = re.compile(r'^\S+-(\d+):\s*(\S+):\s+(.+)')
        hw  = {}
        for line in self.stdout:
            #print(line)
            mo = prwre.search(line)
            if mo:
                node = mo.group(1)
                key = 'PwrSupl_' + mo.group(2)
                val = mo.group(3)
                hw.setdefault(node,{})[key] = val
                continue
            mo = hwre.search(line)
            if mo:
                node = mo.group(1)
                key = mo.group(2)
                val = mo.group(3)
                hw.setdefault(node,{})[key] = val
        ### NEW CODE HERE""
        ### Change mts dict name as needed
        setattr(self,fdata,hw)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def auth_nis_list_csv(self):
        """Get nis info, place into dict
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
        nis = {}
        reader = csv.DictReader(self.stdout)
        for row in reader:
             #pprint.pprint(row)
             #domain = row['NIS Domain']
             name = row['Name']
             #nis[domain] = row
             nis[name] = row
        setattr(self,fdata,nis)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def status_n(self):
        """isi Status -n into dict.
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
        st = {}
        lnnre = re.compile(r'Node LNN:\s+(\d+)')
        colonre = re.compile('^(\S+.*\S+):(\s*(.+)\s*)?')
        lnn = ""
        for line in self.stdout:
            #print(line)
            mo = lnnre.search(line)
            if mo:
                lnn = mo.group(1)
                st.setdefault(lnn,{})['LNN'] = lnn
                continue
            mo = colonre.search(line)
            if mo:
                key = mo.group(1)
                key = re.sub('Node ','',key)
                key = re.sub(' ','_',key)
                val = mo.group(3)
                st.setdefault(lnn,{})[key] = val
                continue
        setattr(self,fdata,st)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def status_q(self):
        """isi Status -q into dict.
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
        st = {}
        #colonre = re.compile('^(\S+\s*\S+):(\s*(.+)\s*)?')
        colonre = re.compile('^(\S+.*\S+):(\s*(.+)\s*)?')
        sizestr = r'([\d\.]+[KMGTP]*)\s+\(([\d\.]+[KMGTP]*) Raw\)\s+([\d\.]+[KMGTP]*)\s+\(([\d\.]+[KMGTP]*) Raw\)'
        usedstr = r'([\d\.]+[KMGTP]*)\s+\(.+\)\s+([\d\.]+[KMGTP]*)\s+\(.+\)'
        usedre = re.compile(usedstr)
        storstr = r'([\d\.]+[KMGTP]*)\/\s+([\d\.]+[KMGTP]*)'
        storre = re.compile(storstr)
        sizere = re.compile(sizestr)
        for line in self.stdout:
            #print(line)
            mo = colonre.search(line)
            if mo:
                st[mo.group(1)] = mo.group(3)
                continue
            if not 'Health' in line and '|' in line and not 'DASR' in line:
                st.setdefault('nodes',{})
                ln = line.split('|')
                ln[0].lstrip()
                ln[0] = "".join(ln[0].split())
                #print(ln)
                ID = ln[0]
                st['nodes'].setdefault(ID,{})
                st['nodes'][ID]['ID'] = ln[0]
                st['nodes'][ID]['IP'] = ln[1]
                st['nodes'][ID]['Heatlh'] = ln[2]
                st['nodes'][ID]['Throughput_In'] = ln[3]
                st['nodes'][ID]['Throughput_Out'] = ln[4]
                st['nodes'][ID]['Throughput_Total'] = ln[5]
                st['nodes'][ID]['HDD_Storage'] = ln[6]
                try:
                    val = ln[7]
                except IndexError:
                    val = '(Diskless)'
                st['nodes'][ID]['SSD_Storage'] = val
        mo = sizere.search(st['Size'])
        if mo:
            st['HDD_Size'] = mo.group(1)
            st['HDD_Raw'] = mo.group(2)
            st['SSD_Size'] = mo.group(3)
            st['SSD_Raw'] = mo.group(4)
        mo = usedre.search(st['Used'])
        if mo:
            st['HDD_Used'] = mo.group(1)
            st['SSD_Used'] = mo.group(2)
        for p,d in st['nodes'].items():
            #print(p,d)
            mo = storre.search(d['HDD_Storage'])
            if mo:
                d['HDD_Storage_Used'] = mo.group(1)
                d['HDD_Storage_Size'] = mo.group(2)
            mo = storre.search(d['SSD_Storage'])
            if mo:
                d['SSD_Storage_Used'] = mo.group(1)
                d['SSD_Storage_Size'] = mo.group(2)
        setattr(self,fdata,st)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def status_q_d(self):
        """isi Status -q -d into dict.
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
        st = {}
        #colonre = re.compile('^(\S+\s*\S+):(\s*(.+)\s*)?')
        colonre = re.compile('^(\S+.*\S+):(\s*(.+)\s*)?')
        sizestr = r'([\d\.]+[KMGTP]*)\s+\(([\d\.]+[KMGTP]*) Raw\)\s+([\d\.]+[KMGTP]*)\s+\(([\d\.]+[KMGTP]*) Raw\)'
        usedstr = r'([\d\.]+[KMGTP]*)\s+\(.+\)\s+([\d\.]+[KMGTP]*)\s+\(.+\)'
        usedre = re.compile(usedstr)
        allspacere = re.compile('^\s+$')
        storstr = r'([\d\.]+[KMGTP]*)\/\s*([\d\.]+[KMGTP]*)'
        storre = re.compile(storstr)
        #print(sizestr)
        #sys.exit()
        sizere = re.compile(sizestr)
        prevpool = None
        for line in self.stdout:
            #print(line)
            mo = colonre.search(line)
            if mo:
                st[mo.group(1)] = mo.group(3)
            if not 'Health' in line and '|' in line:
                st.setdefault('pools',{})
                ln = line.split('|')
                ln[0].lstrip()
                ln[0] = "".join(ln[0].split())
                #print(ln)
                ###Pool name might wrap around to next line
                if allspacere.search(ln[1]):
                    #print(st['pools'][prevpool])
                    pname = prevpool + ln[0]
                    #print(pname)
                    st['pools'][pname] = st['pools'][prevpool]
                    if prevpool in st['pools']:
                        del st['pools'][prevpool]
                    continue
                if not allspacere.search(ln[1]):
                    Pool = ln[0]
                    prevpool = Pool
                    st['pools'].setdefault(Pool,{})
                    st['pools'][Pool]['Heatlh'] = ln[1]
                    st['pools'][Pool]['Throughput_In'] = ln[2]
                    st['pools'][Pool]['Throughput_Out'] = ln[3]
                    st['pools'][Pool]['bps'] = ln[4]
                    st['pools'][Pool]['HDD_Storage'] = ln[5]
                    st['pools'][Pool]['SSD_Storage'] = ln[6]
        mo = sizere.search(st['Size'])
        if mo:
            st['HDD_Size'] = mo.group(1)
            st['HDD_Raw'] = mo.group(2)
            st['SSD_Size'] = mo.group(3)
            st['SSD_Raw'] = mo.group(4)
        mo = usedre.search(st['Used'])
        if mo:
            st['HDD_Used'] = mo.group(1)
            st['SSD_Used'] = mo.group(2)
        for p,d in st['pools'].items():
            #print(p,d)
            mo = storre.search(d['HDD_Storage'])
            if mo:
                d['HDD_Storage_Used'] = mo.group(1)
                d['HDD_Storage_Size'] = mo.group(2)
            mo = storre.search(d['SSD_Storage'])
            if mo:
                d['SSD_Storage_Used'] = mo.group(1)
                d['SSD_Storage_Size'] = mo.group(2)
        setattr(self,fdata,st)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def quota_quotas_list_csv(self):
        """Put quota_list_csv into dict. Out key = path
        """
        fcmd = sys._getframe().f_code.co_name
        skey = "command_quota_quotas_list_csv"
        sval  = "isi --timeout 120 quota list -v --format csv -z"
        #scmd = "quota_list_csv"
        setattr(self.op,skey,sval)
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
        qs = {}
        #for line in self.stdout:
            #print(line)
        reader = csv.DictReader(self.stdout)
        for row in reader:
             #pprint.pprint(row)
             path = row['Path']
             qs.setdefault(path,[]).append(row)
             #qs[path] = row
        setattr(self,fdata,qs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def nfs_paths(self):
        """From nfs_exports_list , make a dict
             with paths as the the primary key.
             npaths[zone] = {}
             npaths[zone][path] = {}
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        #super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        jobj = self.fresh_json(fcmd)
        if jobj :
            setattr(self, fdata, jobj)
            return(getattr(self, fdata))
        npaths = {}
        self.log.info("Getting nfs_exports_list")
        nfs = self.nfs_exports_list
        if not nfs:
             self.log.error("Unable to get nfs_exports_list")
             return None
        for zone,ids in nfs.items():
            for id in ids:
                npaths.setdefault(zone,{})
                paths = ids[id]['Paths']
                pathslist = paths.split(',')
                for p in pathslist:
                    #print(p.lstrip(),id,zone)
                    npaths[zone].setdefault(p.lstrip(),{})
                    #npaths[zone][p.lstrip()]['zone'] = zone
                    try:
                        npaths[zone][p.lstrip()]['ids'] += " " + id
                    except KeyError:
                        npaths[zone][p.lstrip()]['ids'] = id
        setattr(self,fdata,npaths)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def nfs_aliases_list(self):
    ####################################################################
        """Put the nfs aliases into a dict. 
            dict[zone] = {}
            dict[zone][path] = []
            dict[zone][path].append(alias_name)
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
        jobj = self.fresh_json(fcmd)
        if jobj :
            setattr(self, fdata, jobj)
            return(getattr(self, fdata))
        self.log.info("nfs_aliases_list.json not found, getting zones.")
        zs = self.zone_zones_list
        if not zs:
            self.log.error("Unable to get info from zone_zones_lists")
            return None
        a  = {}
        for zone in zs:
            #print(zone)
            skey = "command_nfs_aliases_list_zone_{}".format(zone)
            sval  = "isi --timeout 120 nfs aliases list --zone {} --format csv"\
                    .format(zone)
            scmd = "nfs_aliases_list_zone_{}".format(zone)
            setattr(self.op,skey,sval)
            self.get_file_cmd(scmd)
            if not self.stdout:
                self.log.warn("Unable to get info from ->{}".format(sval))
                continue
            reader = csv.DictReader(self.stdout)
            for row in reader:
                #pprint.pprint(row)
                Zone = row['Zone']
                Path = row['Path']
                a.setdefault(Zone,{})
                a[Zone].setdefault(Path,[]).append(row['Name'])
               
        setattr(self,fdata,a)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def nfs_exports_list(self):
    ####################################################################
        """Put the nfs exports into a dict. Outer key = zone. 
           {zone} => {ID} => {details"
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        #super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        jobj = self.fresh_json(fcmd)
        if jobj :
            setattr(self, fdata, jobj)
            return(getattr(self, fdata))
        self.log.info("nfs_exports_list.json not found, getting zones.")
        zs = self.zone_zones_list
        if not zs:
            self.log.error("Unable to get info from zone_zones_lists")
            return None
        idre = re.compile('^\s*ID:\s+(\d+)')
        maprootre = re.compile('^\s*Map Root')
        mapallre = re.compile('^\s*Map All')
        mapnonrootre = re.compile('^\s*Map Non Root')
        mapfailurere = re.compile('^\s*Map Failure')
        ugstr = '^\s+(User|Primary Group|Secondary Groups):'
        ugstr += '\s(.+)'
        usergroupre = re.compile(ugstr)


        nfs = {}
        for zone in zs:
            #print(zone)
            skey = "command_nfs_exports_list_zone_{}".format(zone)
            sval  = "isi --timeout 120 nfs exports list --zone {} -v"\
                    .format(zone)
            scmd = "nfs_exports_list_zone_{}".format(zone)
            setattr(self.op,skey,sval)
            self.get_file_cmd(scmd)
            if not self.stdout:
                self.log.warn("NO STDOUT")
                self.log.warn("Unable to get info from ->{}".format(sval))
                continue
            currflag = None
            id = None
            #nfs[zone] = {}
            for line in self.stdout:
                mo = idre.search(line)
                if mo:
                    nfs.setdefault(zone,{})
                    id = mo.group(1)
                    currflag = None
                    nfs[zone][id] = {}
                    nfs[zone][id]['ID'] = id
                    continue
                mo = mapallre.search(line)
                if mo:
                    currflag = "Map All"
                    continue
                mo = maprootre.search(line)
                if mo:
                    currflag = "Map Root"
                    continue
                mo = mapnonrootre.search(line)
                if mo:
                    rootflag = True
                    currflag = "Map Non Root"
                    continue
                mo = mapfailurere.search(line)
                if mo:
                    currflag = "Map Failure"
                    continue
                mo = usergroupre.search(line)
                if mo:
                    key = currflag + " " + mo.group(1)
                    val = mo.group(2)
                    nfs[zone][id][key] = val
                    continue
                mo = colonre.search(line)
                if mo:
                    key = mo.group(1)
                    val = mo.group(3)
                    #print("key->{} val=>{}".format(key,val))
                    nfs[zone][id][key] = val
        #pprint.pprint(nfs) ; sys.exit
        setattr(self,fdata,nfs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    def process_smb_shares_list(self,input=None):
    ###################################################################
        """creates a dict from isi smb shares list --zone -v
        """
        sname = None
        smb = {}
        for line in input:
            mo = snamere.search(line)
            if mo:
                sname = mo.group(1)
            mo = colonre.search(line)
            if mo:
                key = mo.group(1)
                val = ""
                if mo.group(3):
                    val = mo.group(3)
                smb.setdefault(sname,{})[key] = val
        return smb        
    ####################################################################
    @property
    def smb_shares_list(self):
    ####################################################################
        """List shares by zone into dict. Outer key = zone
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
        jobj = self.fresh_json(fcmd)
        if jobj :
            setattr(self, fdata, jobj)
            return(getattr(self, fdata))
        self.log.info("smb_shares_list.json not found, getting zones.")
        zs = self.zone_zones_list
        if not zs:
            self.log.error("Unable to get info from zone_zones_lists")
            return None
        smb = {}
        for zone in zs:
            skey = "command_smb_shares_list_zone_{}".format(zone)
            sval  = "isi --timeout 120 smb shares list --zone {} -v".format(zone)
            scmd = "smb_shares_list_zone_{}".format(zone)
            setattr(self.op,skey,sval)
            self.get_file_cmd(scmd)
            if not self.stdout:
                self.log.warn("NO STDOUT")
                self.log.warn("Unable to get info from ->{}".format(sval))
                continue
            sdict = self.process_smb_shares_list(self.stdout)
            if not sdict:
                self.log.error("sdict not returned")
                return None
            for share,vals in sdict.items():
                smb.setdefault(zone,sdict)
        setattr(self,fdata,smb)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def zone_zones_list(self):
    ####################################################################
        """Returns dict of access zones
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        runc = ("command_" + fcmd)
        setattr(self.op,runc,'isi --timeout 120 zone zones list -v')
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
        zs = {}
        name = None
        colonre = re.compile('^\s+(\S.+\S):{1}\s((.+)\s*)?')
        namere = re.compile('^\s+Name\s*:\s*(.+)\s*')
        #print(colonre)
        for line in self.stdout:
            #print(line)
            mo = namere.search(line)
            if mo:
                name = mo.group(1).rstrip()
            mo = colonre.search(line)
            if mo:
                key = mo.group(1).rstrip()
                val = " "
                if mo.group(3):
                    val = mo.group(3).rstrip()
                #print(key,"=>",val)
                zs.setdefault(name,{})[key] = val
        setattr(self,fdata,zs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    def process_smb_shares_view(self,instd=None):
    ###################################################################
        """creates a dict from isi smb shares view <share> --zone.
        """
        sname = None
        smb = {}
        pflag = False #permissions flag
        totalre = re.compile('^\s*Total\s*:\s*\d+')
        permsre = re.compile('^\s*Permissions\s*:')
        permskipre = re.compile('^(Account|--+)')
        for line in instd:
            #print(line)
            mo = snamere.search(line)
            if mo:
                sname = mo.group(1)
            mo = totalre.search(line)
            if pflag and mo:
                pflag = False
            mo = permsre.search(line)
            if mo:
                pflag = True
                continue
            if pflag and permskipre.search(line):
                continue
            if pflag:
                #print("Setting account perms")
                #Account,Account_Type,Run_as_root,Perm_type,Permission = \
                #line.split()
                p = line.split()
                #print('perm',p[-1])
                #print('type',p[-2])
                #print('root',p[-3])
                #print('act_type',p[-4])
                act = " ".join(p[:-4])
                #print('account',act)
                smb.setdefault('Permissions',{})
                smb['Permissions'].setdefault(act,{}) 
                smb['Permissions'][act]['Account'] = act
                smb['Permissions'][act]['Account_Type'] \
                    = p[-4]
                smb['Permissions'][act]['Run_as_root'] \
                    = p[-3]
                smb['Permissions'][act]['Perm_type'] = p[-2]
                smb['Permissions'][act]['Permission'] = p[-1]
                #print(p)
                #pprint.pprint(smb);sys.exit()
            mo = colonre.search(line)
            if mo:
                key = mo.group(1)
                val = ""
                if mo.group(3):
                    val = mo.group(3)
                smb[key] = val
        #pprint.pprint(smb)
        #input("Press Enter to continue...")
        return smb        
    ####################################################################
    @property
    def smb_shares_view(self):
        """smb shares list -a -z to a dict
           Note, this sub correctly gets the access zones first.
           So the outkey key = zone_name
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
        jobj = self.fresh_json(fcmd)
        if jobj :
            setattr(self, fdata, jobj)
            return(getattr(self, fdata))
        sl  = self.smb_shares_list
        if not sl:
            self.log.warn("Unable to get info from smb_shares_lists")
            return None
        smb = {}
        for zone,inner in sl.items():
            #pprint.pprint(inner)
            smb.setdefault(zone,{})
            for share in inner:
                ##a single quote in a share name is legal, 
                ##thus we need to test for quote and escape.
                ##a $ in a share name is legal,
                ##thus we need to test for $ and DOUBLE escape
                cmdshare = share
                cmdshare = share.replace("""'""","""\'""")
                cmdshare = share.replace("""$""","""\\$""")
                scmd = "isi smb shares view \"{}\" --zone {}".format(cmdshare,zone)
                self.cmd(scmd)
                if not self.stdout:
                    self.log.error("No info returned from {}".format(scmd))
                    continue
                sdict = self.process_smb_shares_view(self.stdout)
                share = sdict['Share Name']
                #pprint.pprint(sdict)
                smb[zone].setdefault(share,{})
                smb[zone][share] = sdict
        setattr(self,fdata,smb)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))

    ####################################################################
    @property
    def networks_list_pools(self):
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        self.networks_list_pools_headers = ["Access Zone","Aggregation Mode","Allocation","Description","IPranges","In Subnet","Pool Interfaces","Pool Membership","Ranges","Smart Pool","SmartConnect Connection Policy","SmartConnect Failover Policy  ","SmartConnect Rebalance Policy ","SmartConnect Service Subnet   ","SmartConnect SmartConnect Suspended Nodes  ","SmartConnect Suspended Nodes  ","SmartConnect Time to Live","SmartConnect Zone"]
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            return False
        if not self.stdout:
            return None
        skipre = re.compile(r'^\s*$') #blank lines
        npools = {}
        accessre = re.compile('^\s+Access Zone\s*:\s*(\S+)(\s+\(\d+\))?')
        ippoolre = re.compile(r'^(\S+:\S+)(\s+\-\s(.+))?')
        insubnetre = re.compile('^\s+In Subnet:\s+(.+)')
        allocationre = re.compile('^\s+Allocation:\s+(.+)')
        colonre = re.compile('^\s+(.+)\s*:\s*(.+)\s*')
        dot3re = re.compile('^\s+(.+)\s*\.\.\.\s*(.+)\s*')
        poolmre = re.compile('\s+Pool Membership')
        aggre = re.compile('\s+Aggregation Mode')
        ipstr = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
        ipstr = '^\s+(' + ipstr + '-' + ipstr + ')'
        poolintre = re.compile('^\s+(\d{1,2}:.+)')
        iprangere = re.compile(ipstr)
        sconnectre = re.compile('\s+SmartConnect')
        
        smartconnect = False
        key = ""
        val = ""
        desc = " "
        ippool = ""
        for line in self.stdout:
           if re.search(skipre,line):
              continue
           #print(line)
           #pprint.pprint(ippoolre) ; 
           mo = ippoolre.search(line)
           if mo:
               ippool = mo.group(1)
               desc = " "
               if mo.group(3): desc = mo.group(3)
               ipranges = " "
               poolifs = " "
               smartconnect = False
               key = ""
               val = ""
               self.log.debug("Processing ippool->{}".format(ippool))
               npools[ippool] = {}
               npools[ippool]['Smart Pool'] = ippool
               npools[ippool]['Description'] = desc
               continue
           mo = accessre.search(line)
           if mo:
               npools[ippool]['Access Zone'] = mo.group(1)
               npools[ippool]['Access Zone ID'] = mo.group(1)
               if mo.group(2):
                   npools[ippool]['Access Zone ID'] = mo.group(2)
               continue
           mo = colonre.search(line)
           if mo:
               key = mo.group(1).rstrip()
               val = mo.group(2).rstrip()
           mo = dot3re.search(line)
           if mo:
               key = mo.group(1).rstrip()
               val = mo.group(2).rstrip()
           mo = iprangere.search(line)
           if mo:
               ipranges = ipranges + ' ' + mo.group(1)
               self.log.debug("iprange=>{}".format(ipranges))
           if re.search(poolmre,line):
               npools[ippool][key] = val
               npools[ippool]['IPranges'] = ipranges
               self.log.debug("IPranges=>{}".format(ipranges))
               continue
           mo = poolintre.search(line)
           if mo:
               poolifs = poolifs + ' ' + mo.group(1)   
               key = ""
               val = ""
               continue
           if re.search(aggre,line):
               npools[ippool][key] = val
               npools[ippool]['Pool Interfaces'] = poolifs
               self.log.debug("Pool Interfaces=>{}".format(poolifs))
               continue
           if re.search(sconnectre,line):
               smartconnect = True
               continue
           if smartconnect:
               key = 'SmartConnect ' +  key
           if key and val:
               self.log.debug("setting key=>{} val=>{}".format(key,val))
               #print("setting key=>{} val=>{}".format(key,val))
               npools[ippool][key] = val
           else:
               self.log.debug("key or val missing")
        try:
            self.put_json_file(ifile = fcmd , icontent = npools)
        except AttributeError:
            return None
        setattr(self,fdata,npools)
        return getattr(self,fdata)
    ####################################################################
    @property
    def DEPRECATE_sync_policies_list(self):
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        headersname =  'headers_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            return False
        if not self.stdout:
            return None
        idre = re.compile('^\s+ID:\s(\S+)')
        nonid = re.compile('^\s*(.+):\s(.+)')
        policies = {}
        headers = {}
        setattr(self, headersname, {})
        for line in self.stdout:
             #print(line)
             mo = idre.match(line)
             if mo:
                id = mo.group(1)
                policies[id] = {}
                continue
             mo = nonid.match(line)
             if mo:
                fieldname = mo.group(1)
                headers[fieldname] = 1
                fieldvalue = mo.group(2)
                policies[id][fieldname] = fieldvalue
       #pprint.pprint(policies)
        self.put_json_file(ifile = fcmd , icontent = policies)
        if headers:
             setattr(self, headersname, headers)
        setattr(self,fdata,policies)
        return getattr(self,fdata)
    ####################################################################
    @property
    def sync_reports_list(self):
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        headersname =  'headers_' + fcmd
        super().get_stub(fcmd)
        try:
            return  getattr(self, fdata)
        except AttributeError:
            pass
        else:
            return False
        if not self.stdout:
            return None
        policyre = re.compile('^\s+Policy Name:\s(\S+)')
        jobidre = re.compile('^\s+Job ID:\s(\S+)')
        nonidre = re.compile('^\s*(.+):\s(.+)')
        phase = None #holder for the inner key  phases sections
        #phasere = re.compile('^\s*Phases$')
        #phasesflag = False
        policies = {} ##outer key = policy name, inner key = jobid
        policy = "" ##The current working policy name
        jobid = "" ##The current working job ID
        headers = {}
        for line in self.stdout:
            #print(line)
            mo = policyre.search(line)
            if mo:
                policy = mo.group(1)
                if not policy in policies:
                     policies[policy] = {}
                phase = None
                self.log.debug("WORKING POLICY => {}".format(policy))
                continue
            mo = jobidre.search(line)
            if mo:
                jobid = mo.group(1)
                if not jobid in policies[policy]:
                     policies[policy][jobid] = {}
                policies[policy][jobid]['Policy Name'] = policy
                self.log.debug("WORKING JOBID => {}".format(jobid))
            mo = nonidre.search(line)
            if mo:
                addkey  = mo.group(1)
                addvalue = mo.group(2)
                if addkey == 'Phase': 
                    phase = addvalue
                    continue
                if phase:
                    addkey = phase + ' ' + addkey
                policies[policy][jobid][addkey] = addvalue
                setattr(self, headersname, {})
                headers[addkey] = 1
        if headers:
             setattr(self, headersname, headers)
        return policies
    ####################################################################
    @property
    def cluster_identity(self):
        """Cluster Identity stored as json, returned as dict
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
        mts = {}
        mts = json.loads(self.stdout[0])[0]
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def cluster_contact(self):
        """Cluster contact stored as json, returned as dict
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
        mts = {}
        mts = json.loads(self.stdout[0])[0]
        setattr(self,fdata,mts)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def sync_policies_list(self):
        """Get isi sync policies list into object attribute
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
        jsonIn = {}
        jsonOut = {}
        jsonIn = json.loads(self.stdout[0])
        for policy in jsonIn:
            self.log.debug('sync policy',policy['name'])
            jsonOut[policy['id']] = {}
            for key,val in policy.items():
               jsonOut[policy['id']][key] = val
        setattr(self,fdata,jsonOut)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
###########################################################################
    @property
    def headers_sync_policies_list(self):
       """table column headers for sync policies
       """
       fcmd = sys._getframe().f_code.co_name
       fdata = '_' + fcmd
       try:
          return  getattr(self, fdata)
       except AttributeError:
          pass
       policies = self.sync_policies_list
       headers = ['name','has_sync_state','last_job_state']
       headers.extend (['source_root_path','source_include_directories'])
       headers.extend(['source_exclude_directories','source_pool'])
       headers.extend(['target_host','target_path'])
       for polid,policy in policies.items(): 
          for key in sorted(policy.keys()):
             if key not in headers:
                headers.append(key)
          break
       setattr(self, fdata , headers)
       return(getattr(self, fdata))
    ####################################################################
    @property
    def headers_zone_zones_json(self):
        """Take the keys from json and return a list of unique keys.
           Used for table headers.
           Headers will be sorted, with a few specified at start.
        """
        try:
           zonesjson  = self.zone_zones_json
        except Exception as err:
            self.log.error("Unexpected error {}".format(err))
            return False
        atFront = ['name','path','auth_providers']
        oneZoneName  = str(list(zonesjson.values())[0]['name'])
        oneZoneKeys = zonesjson[oneZoneName].keys()
        return (gHeaders(oneZoneKeys,atFront))
    ####################################################################
    @property
    def zone_zones_json(self):
       """Get isi zone zones list into object attribute.
          Turn the list->dict returned from Isilon
          into dict->dict with zone name as outer key.
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
       jsonIn = json.loads(self.stdout[0])
       jsonOut = {}
       for zone in jsonIn:
           self.log.debug('name',zone['name'])
           jsonOut[zone['name']] = {}
           for key,val in zone.items():
              jsonOut[zone['name']][key] = val
       setattr(self,fdata,jsonOut)
       try:
          self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
       except AttributeError:
          return None
       return(getattr(self, fdata))
    ####################################################################
    @property
    def headers_storagepool_list(self):
        """Take the keys from json and return a list of unique keys.
           Used for table headers.
           Headers will be sorted, with a few specified at start.
        """
        try:
           spool_list = self.storagepool_list
        except Exception as err:
            self.log.error("Unexpected error {}".format(err))
            return False
        atFront = ['name','lnns','protection_policy','pct_used_hdd','pct_used_ssd']
        onePoolName  = str(list(spool_list.values())[0]['name'])
        onePoolKeys = spool_list[onePoolName].keys()
        return (gHeaders(onePoolKeys,atFront))
    ####################################################################
    @property
    def storagepool_list(self):
       """Get storagepool list into object attribute.
          Turn the list->dict returned from Isilon
          into dict->dict with pool name as outer key.
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
       jsonIn = json.loads(self.stdout[0])
       #pprint.pprint(jsonIn);sys.exit()
       jsonOut = {}
       for pool in jsonIn:
           poolName = pool['name']
           jsonOut[poolName] = {}
           self.log.debug("Pool name {}".format(poolName))
           for key,val in pool.items():
              self.log.debug("Procssing {} -> {}".format(key,val))
              if key == 'usage':
                 for usage,uval in sorted(val.items()):
                    if '_bytes' in usage:
                       gbVal = round(util.to_gb(uval,'b'),2)
                       gbUsage = usage.replace('_bytes','_gb')
                       jsonOut[poolName][gbUsage] = gbVal
                    else:
                       jsonOut[poolName][usage] = uval
                 continue
              jsonOut[poolName][key] = val
           self.log.debug(pprint.pprint(jsonOut))
           self.log.debug("Done with pool {}".format(poolName))
       setattr(self,fdata,jsonOut)
       try:
          self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
       except AttributeError:
          return None
       return(getattr(self, fdata))
    ####################################################################
    @property
    def sync_rules_list(self):
       """Get isi sync rules list into object attribute 
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
       rules = {}
       reader = csv.DictReader(self.stdout)
       for row in reader:
          for key,val in row.items():
             rules.setdefault(row['ID'],{})[key] = val
       setattr(self,fdata,rules)
       try:
          self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
       except AttributeError:
          return None
       return(getattr(self, fdata))
    ####################################################################
    @property
    def headers_sync_rules_list(self):
       """table column headers for sync rules
       """
       fcmd = sys._getframe().f_code.co_name
       fdata = '_' + fcmd
       try:
          return  getattr(self, fdata)
       except AttributeError:
          pass
       headers = []
       for id,rule in self.sync_rules_list.items(): 
          for key in rule.keys():
             if key not in headers:
                headers.append(key)
          break
       setattr(self, fdata , headers)
       return(getattr(self, fdata))

###########################################################################
__version__ = 2.30
###########################################################################
def history():
    """0.00 Initial
       0.10 sync_reports_list(self):
       0.20 networks_list_pools
       0.30 smb_shares_view, smb_shares_list, zone_zones_list
       0.40 nfs_exports_list
       0.50 nfs_paths
       0.60 quota_quotas_list_csv
       0.70 status_q_d
       0.71 zone_zones_list now accounts for colon in Auth Providers value
       0.80 auth_nis_list_csv
       0.81 fix for HDD_USED in status_q_d
       0.90 version
       1.00 status_q
       1.10 hw_status
       1.10 status_n
       1.11 Fix to colonre, [^:]*
       1.20 snapshot_snapshots_list
       1.30 snapshot_schedules_list
       1.31 fix for smb/nfs list zones, continue if missing data not return.
       1.40 nfs_aliases_list
       1.41 fix to global colonre
       1.42 modification to accessre in netpools_table becauses sometimes
            the isilon does not have a (Access Zone id) in Access Zone: line 
            of isi_classic networks list pools -v
       1.43 fix for status_q_d using isi status -q -p
       1.44 smb_shares_view modifed to account for ' in share name
       1.50 filepool_policies
       1.51 filepool_policies fix for spaces in name
       1.52 fix for smb_shares_view and double $$ in share name
       1.60 added cloud_accounts
       1.70 added cloud_pools
       1.71 isi_smb_shares_view now only warns if no smb shares found
       1.80 isi_cluster_identity and isi_cluster_contact
       1.90 isi_sync_policies_list
       2.00 isi_sync_rules_list
       2.01 bug fix for process_smb_shares_view -> account type
       2.10 zone_zones_json
       2.20 storagepool_list
       2.30 event_group_list
    """
    pass

