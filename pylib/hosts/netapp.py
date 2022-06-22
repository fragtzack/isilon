#import logging
import pprint
import sys
import re
import copy

from .host import Host

blankre = re.compile(r'^\s*$') #blank lines

class Netapp(Host):
    """Netapp type child class of host class."""

    ####################################################################
    def __init__(self,name):
    ####################################################################
        super().__init__(name)
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
    @property
    def nis_info(self):
    ####################################################################
        """\nnis info
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
        domainre = re.compile('^\s*NIS\s(\w*\s*)is\s(\S.+)')
        iplinere = re.compile('^\d{1,3}\.\d{1,3}')
        for line in self.stdout:
            #print(line)
            mo = domainre.search(line)
            if mo:
                nis['domain'] = mo.group(2)
            if iplinere.search(line):
                a = line.split()   
                nis['ip'] = a[0]
                nis['type'] = a[1]
                nis['state'] = a[2]
                nis['bound'] = a[3]
        setattr(self,fdata,nis)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def options(self):
    ####################################################################
        """\nGet options and return simple hash.
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
        o = {}
        optionsre = re.compile('^(\S+)\s+(\S.+)')
        for line in self.stdout:
            #print(line)
            mo = optionsre.search(line)
            if mo:
                key = mo.group(1)
                o[key] = mo.group(2)
            else:
                o[line] = ""
        setattr(self,fdata,o)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def hostname(self):
    ####################################################################
        """\nGet and return hostname and as string
             
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
        h = None
        for line in self.stdout:
            #print(line)
            h = line
        setattr(self,fdata,h)
        return(getattr(self, fdata))
    ####################################################################
    @property
    def quota_report(self):
    ####################################################################
        """\nPut quota report into a dict.
              
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
        qr  = {}
        srre = re.compile('^(  |--|Type)')
        tstr = '^tree\s+(\*|\d+)\s+(\S+)\s+(\w.+\w|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)\s+(\*|\S.+)'
        treere= re.compile(tstr)
        for line in self.stdout:
            if srre.search(line):
                continue
            if 'tree' not in line: continue
            #if 'pgpvol02' not in line: continue
            #print(line)
            #print("LENGTH",len(line.split()))
            mo = treere.search(line)
            if mo:
                id = mo.group(1)
                vol = mo.group(2)
                tree = mo.group(3)
                used = mo.group(4)
                limit = mo.group(5)
                slimit = mo.group(6)
                thold = mo.group(7)
                files = mo.group(8)
                flimit = mo.group(9)
                fslimit = mo.group(10)
                spec = mo.group(11)
                #print("################################")
                #print("id=>{} vol=>{} tree=>{} used=>{} limit=>{} slimit=>{} thold=>{} files=>{} flimit=>{} fslimit=>{} spec=>{}"
                      #.format(id,vol,tree,used,limit,slimit,thold,
                              #files,flimit,fslimit,spec))
                #print("################################")
                id = mo.group(1)
                vol = mo.group(2)
                tree = mo.group(3)
                used = mo.group(4)
                limit = mo.group(5)
                slimit = mo.group(6)
                thold = mo.group(7)
                files = mo.group(8)
                flimit = mo.group(9)
                fslimit = mo.group(10)
                spec = mo.group(11)
                qr.setdefault(vol,{})
                qr[vol].setdefault(tree,{})
                qr[vol][tree]['id'] = id
                qr[vol][tree]['vol'] = vol
                qr[vol][tree]['tree'] = tree
                qr[vol][tree]['used'] = used
                qr[vol][tree]['limit'] = limit
                qr[vol][tree]['slimit'] = slimit
                qr[vol][tree]['thold'] = thold
                qr[vol][tree]['files'] = files
                qr[vol][tree]['flimit'] = flimit
                qr[vol][tree]['fslimit'] = fslimit
                qr[vol][tree]['spec'] = spec 
        #pprint.pprint(qr)
        #sys.exit()
        setattr(self,fdata,qr)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def cifsconfig_share(self):
    ####################################################################
        """\nPut the cifs shares into a hash. Access entries ignored 
             for now.
        """
        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
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
        cifs = {}
        shstr = '^cifs shares -add "(.+)" "(.+)" -comment "(.*)"'
        shre = re.compile(shstr)
        for line in self.stdout:
            #if 'PASCI' not in line: continue
            #print(line)
            mo = shre.search(line)
            if mo:
                sh = mo.group(1)
                cifs[sh] = {}
                cifs[sh]['mnt'] = mo.group(2)
                cifs[sh]['desc'] = mo.group(3)
        setattr(self,fdata,cifs)
        #return cifs
        #pprint.pprint(cifs)
        #sys.exit()
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def quota_status(self):
    ####################################################################
        """\nQuota status into a dict
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
        qs = {}
        qsre = re.compile('^(\S+): quotas are (\S+)\.')
        for line in self.stdout:
            #print(line)
            mo = qsre.search(line)
            if mo:
                qs[mo.group(1)] = mo.group(2)
        setattr(self,fdata,qs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))

    ####################################################################
    @property
    def vol_status_v(self):
    ####################################################################
        """\nvol status -v into dict
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
        vs = {}
        vol = None
        slre = re.compile('^\s+Volume State\s+(\w\S+,\s\S+)\s+\S+')
        vstr  = '^\s*(\S+)\s(online|offline|restricted)\s+'
        vstr  = vstr + '((raid0|raid4|raid_dp), (flex|traditional))'
        vstatusre = re.compile(vstr)
        #vstatusre = re.compile('^\s+(\S+)\s(online|offline|restricted)\s+')
        compressre = re.compile('compression=(\S+),')
        guaranteere = re.compile('guarantee=(\S+),')
        aggrre = re.compile('Containing aggregate: \'(\S+)\'')
        autore = re.compile('Volume autosize settings')
        autostatere = re.compile('state=(\S+)')
        autosizere = re.compile('(maximum-size|increment-size)=(\S+\s\S+)')
        statestr = "\s(copying|degraded|flex|foreign|growing|"
        statestr = statestr + "initializing|invalid|ironing|"
        statestr = statestr + "mirror degraded|mirrored|needs check|"
        statestr = statestr + "out-of-date|partial|reconstruct|"
        statestr = statestr + "redirect|resyncing|snapmirrored|"
        statestr = statestr + "sv-restoring|trad|unrecoverable|"
        statestr = statestr + "verifying|wafl inconsistent|flexcache|"
        statestr = statestr + "connecting|sis)\s"
        statere = re.compile(statestr)

        auto = False
        for line in self.stdout:
            if slre.search(line): continue
            #print(line)
            mo = vstatusre.search(line)
            if mo:
                vol = mo.group(1)
                state = mo.group(2)
                type = mo.group(3)
                #status = mo.group(3)
                #print("###################vol=>{} state=>{} type=>{}" 
                     #.format(vol,state,type))
                auto = False
                vs[vol] = {}
                vs[vol]['name'] = vol
                vs[vol]['state'] = state
                vs[vol]['type'] = type
                vs[vol]['maximum-size'] = None
                vs[vol]['increment-size'] = None
                vs[vol]['status'] = ""
                continue
            mo = statere.search(line)
            if mo:
                sstate = mo.group(1)
                #vs[vol]['status'] = vs[vol]['status'] + " " + sstate
                vs[vol]['status'] += ' ' + sstate
                vs[vol]['status'] = vs[vol]['status'].strip()
            mo = compressre.search(line)
            if mo:
                compression = mo.group(1)
                #print("###################compression =>{}".format(compression))
                vs[vol]['compression'] = compression
                continue
            mo = guaranteere.search(line)
            if mo:
                guarantee = mo.group(1)
                #print("###################guarantee =>{}".format(guarantee))
                vs[vol]['guarantee'] = guarantee
                continue
            mo = aggrre.search(line)
            if mo:
                aggr = mo.group(1)
                #print("###################aggr => {}".format(aggr))
                vs[vol]['aggr'] = aggr
                continue
            mo = autore.search(line)
            if mo:
                auto = True
                continue
            #if auto :
                #print("AUTO")
            mo = autostatere.search(line)
            if auto and mo:
                autostate = mo.group(1)
                #print("###################autostate=> {}".format(autostate))
                vs[vol]['autostate'] = autostate
                continue
            mo = autosizere.search(line)
            if mo:
                autosize = mo.group(1)
                size = mo.group(2)
                #print("###################auto {}=> {}".
                      #format(autosize,size))
                vs[vol][autosize] = size
                continue
 
        setattr(self,fdata,vs)
        return(vs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def df_a(self):
    ####################################################################
        """\nGet df -A and return a dict.
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
        df  = {}
        skpre = re.compile('^Aggregate ')
        dfre = re.compile("^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)")
        for line in self.stdout:
            if skpre.search(line): continue
            #print(line)
            mo = dfre.search(line)
            if mo:
                aggr = mo.group(1)
                kb = mo.group(2)
                used = mo.group(3)
                avail = mo.group(4)
                capacity = mo.group(5)
                df[aggr] = {}
                df[aggr]['aggr'] = aggr
                df[aggr]['kb'] = kb
                df[aggr]['used'] = used
                df[aggr]['avail'] = avail
                df[aggr]['capacity'] = capacity
        setattr(self,fdata,df)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def df(self):
    ####################################################################
        """\nGet df and return a dict.
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
        df  = {}
        skpre = re.compile('^Filesystem')
        srre = re.compile("^snap reserve")
        dfstr = "^(snap reserve|/vol/\S+)\s+(\S+)\s+(\S+)\s+(\S+)"
        dfstr = dfstr + "\s+(\S+)\s+(\S+)"
        #dfstr = "^(/vol/\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"
        dfre = re.compile(dfstr)
        for line in self.stdout:
            if skpre.search(line): continue
            if srre.search(line): continue
            #print(line)
            mo = dfre.search(line)
            if mo:
                fs = mo.group(1)
                kb = mo.group(2)
                used = mo.group(3)
                avail = mo.group(4)
                capacity = mo.group(5)
                mount = mo.group(6)
            #fs,kb,used,avail,capacity,mount = line.split()
                df[mount] = {}
                df[mount]['fs'] = fs
                df[mount]['kb'] = kb
                df[mount]['used'] = used
                df[mount]['avail'] = avail
                df[mount]['capacity'] = capacity
                df[mount]['mount'] = mount
        setattr(self,fdata,df)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def qtree_status(self):
    ####################################################################
        """\nqtree status. Returns dict of full path outer key.
             For example: 
             /vol/vol0 or /vol0/pgpvol01 or /vol/pgpvol01/ISIS-Reports
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
        qs = {}
        skre = re.compile('^(Volume|--)')
        volre = re.compile('^(\w\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$')
        #treere = re.compile('^(\w\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)')
        treestr = '^(\w\S+)\s+(\w.+\S)\s+(unix|ntfs|mixed)\s+(\S+)'
        treestr += '\s+(\S+)'
        treere = re.compile(treestr)
        for line in self.stdout:
            if skre.search(line): continue
            #if 'pacone' not in line: continue
            #print(line)
            mo = volre.search(line)
            if mo:
               vol = mo.group(1)
               key = "/vol/{}".format(vol)
               style = mo.group(2)
               oplocks = mo.group(3)
               status = mo.group(4)
               qs[key] = {}
               qs[key]['vol'] = vol
               qs[key]['style'] = style
               qs[key]['oplocks'] = oplocks
               qs[key]['status'] = status
               continue
            mo = treere.search(line)
            if mo:
               vol = mo.group(1)
               tree = mo.group(2)
               tkey = "/vol/{}/{}".format(vol,tree)
               qs[tkey] = {}
               qs[tkey]['vol'] = vol
               qs[tkey]['tree'] = tree
               qs[tkey]['style'] = mo.group(3)
               qs[tkey]['oplocks'] = mo.group(4)
               qs[tkey]['status'] = mo.group(5)
        #pprint.pprint(qs)
        #sys.exit()
        setattr(self,fdata,qs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def exportfs(self):
    ####################################################################
        """\nGets and returns exportfs as a dict.
            Keys = {path} -> {details}
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
        nfs = {}
        path = ""
        linere = re.compile(r'^(/vol/\S+)\s+(\S+)')
        for line in self.stdout:
            #print(line)
            mo = linere.search(line)
            if mo:
                path = mo.group(1)
                opts = mo.group(2)
                #print("###################")
                #print("PATH=>{} OPTS={}".format(path,opts))
                dopts = process_exportfs_opts(path,opts)
                nfs[path] = dopts
                nfs[path]['path'] = path
                #pprint.pprint(dopts)
           
        setattr(self,fdata,nfs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def cifs_shares(self):
    ####################################################################
        """\nGet cifs shares and returns a dict of {share_name} -> {info}
        """
        ## this function retired because of spaces in tree paths
        ## use cifsconfig_share instead
        return
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
        cifs = {}
        sharere = re.compile(r'^(\S.+?)\s+(/etc|/vol/\S+|/)\s*(\S.*)*')
        accessre = re.compile(r'^\s+(\S.*)\s/\s(\S.+)')
        shr = ""
        for line in self.stdout:
            #Digital_Marketing
            #if 'PASCI' or 'CompPlanning' not in line:
            if 'PASCI' not in line:
                continue
            print(line)
            mo = sharere.search(line)
            if mo:
                shr = mo.group(1)
                mnt = mo.group(2)
                desc = mo.group(3)
                cifs[shr] = {}
                cifs[shr]['mnt'] = mnt
                cifs[shr]['desc'] = desc
                #print("##########################")
                #print("SHR=>",shr,"MNT=>",mnt,"DESC=>",desc)
                continue
            mo = accessre.search(line)
            if mo:
                user = mo.group(1)
                access = mo.group(2)
                cifs[shr].setdefault('access',{})
                #cifs[shr]['user'] = user
                cifs[shr]['access'][user] = access
                #print("USER=>{} ACCESS=>{}".format(user,access))
        pprint.pprint(cifs) ; sys.exit()
        setattr(self,fdata,cifs)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def snapmirror_status_l(self):
    ####################################################################
        """\nPut snapmirror status -l into a dict.
             Only 2 keys: status and mirrors. [s] and [m]
             status is a string, mirrors is a list of dicts.
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
        colonre = re.compile('^(\S.+\S):{1}\s+(\S.*)')
        sourcere = re.compile('^Source:\s+(\S.+)')
        snapmirrorre = re.compile('^Snapmirror is (\S.+)')
        sm = {}
        d = {}
        for line in self.stdout:
            #print(line)
            mo = snapmirrorre.search(line)
            if mo:
                sm['s'] = mo.group(1)
            mo = colonre.search(line)
            if mo:
                key = mo.group(1)
                val = mo.group(2)
                #print("COLON HIT=>{}".format(val))
                sm.setdefault('m',[])
                d[key] = val
                continue
            mo = blankre.search(line)
            if mo:
                if d:
                    sm['m'].append(d)
                    d = {}
        #pprint.pprint(sm)
        #sys.exit()
        setattr(self,fdata,sm)
        try:
            self.put_json_file(ifile= fcmd ,
                               icontent=getattr(self,fdata))
        except AttributeError:
            return None
        return(getattr(self, fdata))
    ####################################################################
    @property
    def ifconfig(self):
    ####################################################################
        """\nGets COMMAND ifconfig -a and returns data in dict.
           AT THIS TIME, ONLY RETURNS AN IP->DEVICE dict
        """

        fcmd = sys._getframe().f_code.co_name
        fdata = '_' + fcmd
        self.ifconfig_header = \
              "Device IP".split()
        fields= self.ifconfig_header
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
        try:
            getattr(self,fdata)
        except AttributeError:
            setattr(self,fdata,{})
        
        devre = re.compile('^(\S+):\s+flags')
        ipre = re.compile('^\s+inet\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+')
        dev = ""
        ip = ""
        ifs = {}
        for line in self.stdout:
            #print(line)
            mo = devre.search(line)
            if mo:
                dev = mo.group(1)
                #print("dev=>{}".format(dev))
                continue
            mo = ipre.search(line)
            if mo:
                ip = mo.group(1)
                #print("ip=>{}".format(ip))
                ifs[ip] = dev
                continue
        setattr(self,fdata,ifs)
        try:
            self.put_json_file(ifile= fcmd ,icontent=ifs)
        except AttributeError:
            return None
        return getattr(self,fdata)
    #################################################################### 
    def ls_homedirs(self):
    ####################################################################
        """\nPerform an ls on all the path locations from self.cifs_homedir

        Record the output in the host cache ../var/db/<host>
        as file name 'homedirs.ls_path.txt'
        Creates a dict of dict of dicts called self.lshomedirs. 
        Here are the keys:
             homedir : {}
                       user : {}
        """

        fcmd = 'lshomedirs'
        try:
            return  getattr(self, fcmd)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        jobj = self.fresh_json(fcmd)
        if jobj :
            return(setattr(self, fcmd, jobj))
        if not self.get_cifs_homedir():
            return False
        self.lshomedirs = {}
        for homedir in self.cifs_homedir:
            print(homedir)
            self.lshomedirs[homedir] = {}
            mycmd = "priv set -q advanced; ls {}".format(homedir)
            self.cmd(mycmd)
            #for some reason this command comes through stderr
            if not self.stderr:
                return False
            for user in self.stderr:
                 if user == """..""" or user == """.""":
                     continue
                 self.lshomedirs[homedir][user] = {}
        try:
            self.put_json_file(ifile= fcmd ,
                              icontent=getattr(self,fcmd))
        except AttributeError:
            return False
        return self.lshomedirs
    #################################################################### 
    def get_cifs_homedir(self):
    ####################################################################
        """\nGets COMMAND cifs_homedir and places data into dict.

           dict = self.cifs_homedir
           The keys will be locations, the values will all be 1.
        """

        fcmd = sys._getframe().f_code.co_name[4:]
        super().get_stub(fcmd)
        try:
            return  getattr(self, fcmd)
        except AttributeError:
            pass
        else:
            self.log.error("Unexpected error {}".format(err))
            return False
        if not self.stdout:
            return False
        self.cifs_homedir = {}
        for line in self.stdout:
            #print(line)
            if line == 'No CIFS home directory paths.' :
                return None
            self.cifs_homedir[line]=1
        try:
            self.put_json_file(ifile= fcmd ,
                              icontent=getattr(self,fcmd))
        except AttributeError:
            return False
        return getattr(self,fcmd)
anonre = re.compile(r'^-*anon=(\S+)$')
actualre = re.compile(r'^-*actual=(\S+)$')
secre = re.compile(r'^-*sec=(\S+)$')
nosuidre = re.compile(r'^-*nosuid$')
rwre = re.compile(r'^-*rw=(\S+)$')
rore = re.compile(r'^-*ro=(\S+)$')
rootre = re.compile(r'^-*root=(\S+)$')
allrwre = re.compile(r'^-*rw$')
allrore = re.compile(r'^-*ro$')
allrootre = re.compile(r'^-*root$')
###########################################################################
def process_exportfs_opts(path,opts):
###########################################################################
    #print("IN OPTS",opts)
    d = {}
    d['actual'] = path
    d['sec'] = None
    d['ro'] = None
    d['rw'] = None
    d['root'] = None
    d['nosuid'] = None
    d['anon'] = None
    opts = opts.split(',')
    for op in (opts):
        mo = actualre.search(op)
        if mo:
            d['actual'] =   mo.group(1)
        mo = secre.search(op)
        if mo:
            d['sec'] =   mo.group(1)
        mo = nosuidre.search(op)
        if mo:
            d['nosuid'] = True
        mo = rore.search(op)
        if mo:
            d['ro'] = mo.group(1)
        mo = rwre.search(op)
        if mo:
            d['rw'] = mo.group(1)
        mo = rootre.search(op)
        if mo:
            d['root'] = mo.group(1)
        mo = allrore.search(op)
        if mo:
            d['ro'] = 'Open to all'
        mo = allrwre.search(op)
        if mo:
            d['rw'] = 'Open to all'
        mo = allrootre.search(op)
        if mo:
            d['root'] = 'Open to all'
        mo = anonre.search(op)
        if mo:
            d['anon'] = mo.group(1)
    return(d)
__version__ = 1.31
###########################################################################
def history():
###########################################################################
    """\n0.10 Initial
         0.11 get_cifs_homedir
         0.12 ls_homedirs
         0.20 ifconfig
         0.30 cifs_shares
         0.40 exportfs
         0.50 qtree_status
         0.60 df
         0.70 vol_status_v
         0.80 df_A
         0.90 quota_status and quota_report
         0.91 qtree_status changed to account for spaces in tree
         0.92 cifs_shares disabled cifsconfig_share added
         0.93 qtree_status fixed to not add extra space
         1.00 snapmirror_status
         1.10 hostname
         1.20 options
         1.30 nis info 
         1.31 fix for nis info being turned off
    """
    pass

