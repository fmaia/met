'''
Copyright (c) 2012.

Universidade do Minho
Francisco Cruz
Francisco Maia
Joao Paulo
Miguel Matos
Ricardo Vilaca
Jose Pereira
Rui Oliveira

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
'''
import copy
import logging
import math
import time
import Actuator
import decisionmaker_config
import operator

class DecisionMaker(object):



    def __init__(self,stats):
        self._machtoadd = 1
        self._machtoaddBefore = 1 #set to 0 if Fibonacci sequence from the beggining
        self._reconfigure = True
        self._stats = stats
        self._actuator = Actuator.Actuator(self._stats)
        #current state of the system - initially is empty
        self._machine_type = {}
        self._current_config = {}

        #DECISION MAKER PARAMETERS
        self._CPU_IDLE_MIN = decisionmaker_config.cpu_idle_min
        self._CPU_IDLE_MAX = decisionmaker_config.cpu_idle_max
        self._IO_WAIT_MAX = decisionmaker_config.io_wait_max
        self._CRITICAL_PERC = decisionmaker_config.critialStatePercentage
        self._READ_WRITE_DISTANCE_MIN = decisionmaker_config.read_write_distance_min
        self._MIN_SCAN_RATIO = decisionmaker_config.min_scan_ratio

        logging.info('DecisionMaker started.')


    #UTIL METHODS -----------------------------------------------------------------------------------------------------

    def isRegionServerDying(self,rstats):
        res = False
        #condition that evaluates if the RegionServer is overloaded
        if float(rstats['cpu_idle']) < self._CPU_IDLE_MIN or float(rstats['cpu_wio']) > self._IO_WAIT_MAX:
            logging.info('cpu_idle:'+str(rstats['cpu_idle'])+" cpu_wio:"+str(rstats['cpu_wio']))
            res = True
        return res

    def isRegionServerExtra(self,rstats):
        res = False
        if float(rstats['cpu_idle']) > self._CPU_IDLE_MAX:
            logging.info('checking extra -> cpu_idle:'+str(rstats['cpu_idle'])+" cpu_wio:"+str(rstats['cpu_wio']))
            res = True
        return res

    def tagRegion(self,rstats,previousrstats=None):
        tag = 'rw'
        if previousrstats == None:
            scantsize = float(rstats[2])
            reads = float(rstats[0])
            writes = float(rstats[1])
        else:
            scantsize = float(rstats[2]) - float(previousrstats[2])
            reads = float(rstats[0]) -  float(previousrstats[0])
            writes = float(rstats[1]) - float(previousrstats[1])
            if scantsize < 0:
                scantsize = float(rstats[2])
            if reads < 0:
                reads = float(rstats[0])
            if writes < 0:
                writes = float(rstats[1])

        totalreqs = reads + writes

        if reads == 0.0:
            reads = scantsize

        if reads == 0.0:
            scanratio = 0.0
        else:
            scanratio = scantsize /reads

        if scanratio >= self._MIN_SCAN_RATIO:
            tag = 's'
        else:
            if totalreqs!=0:
                percReads = reads / totalreqs
                percWrites = writes / totalreqs
                if math.fabs(percReads-percWrites) > self._READ_WRITE_DISTANCE_MIN:
                    if percReads > percWrites:
                        tag = 'r'
                    else:
                        tag = 'w'

        return tag,totalreqs


    def isHalf(self,v):
        v = round(v,1)
        mv = v + 0.5
        rm = mv % 1
        if rm == 0:
            return True
        else:
            return False

    #ASSIGN MACHINES TO TYPES OF HBASE NODE CONFIGURATIONS
    def tagging(self,regionStats,previousRegionStats,nregionservers,typeDying={}):

        regionTags = {}
        tag_count = {'rw':0,'s':0,'r':0,'w':0}
        tag_order = ['rw','s','r','w']
        nregions = 0

        #tag each region according to request patterns
        for region in regionStats.keys():
            if not region.startswith('-ROOT') and not region.startswith('.META'):
                if previousRegionStats == {}:
                    tag_,reqs = self.tagRegion(regionStats[region])
                else:
                    tag_,reqs = self.tagRegion(regionStats[region],previousRegionStats[region])
                regionTags[region] = (tag_,reqs)
                if (reqs>0):
                    tag_count[tag_] = tag_count[tag_] + 1
                    nregions = nregions + 1

        #calculate the number of machines to assign to each tag
        machines_per_tag = {}
        machines_per_tag_float = {}
        flagged = []
        res_total = 0
        for tag in tag_order:
            tmp_perc =  float(tag_count[tag]) / float(nregions)
            tempvalue = tmp_perc * nregionservers
            flagg = self.isHalf(tempvalue)
            if flagg:
                flagged.append(tag)
            machines_ = round(tempvalue)
            machines_per_tag[tag] =	machines_
            machines_per_tag_float[tag] = tempvalue
            res_total = res_total + machines_



        #treat the case where the round function originates errors
        serverdiff = res_total - nregionservers
        if serverdiff>0 :
            #need to remove machines
            if not flagged:
                min_perc = machines_per_tag_float['rw']
                min_tag = 0
                for tag in tag_order:
                    if machines_per_tag_float[tag]<=min_perc:
                        min_perc = machines_per_tag_float[tag]
                        min_tag = tag
                machines_per_tag[min_tag] = machines_per_tag[min_tag]-1
            else:
                for i in range(0,int(serverdiff)):
                    tagtouse = flagged.pop()
                    if machines_per_tag[tagtouse]>0:
                        machines_per_tag[tagtouse] = machines_per_tag[tagtouse]-1

        elif serverdiff<0 :
            #need to add machines
            temp = sorted(machines_per_tag_float.iteritems(), key=operator.itemgetter(1))
            temp.pop()
            pt = None
            for i in temp:
                if pt == None:
                    pt = i
                else:
                    if i[1] > pt[1]:
                        pt = i
                    elif i[0]=='r':
                        pt = i

            machines_per_tag[pt[0]] =  machines_per_tag[pt[0]] + abs(serverdiff)


        logging.info('Number of Regions with Reqs > 0: '+str(nregions))
        logging.info('Number of RegionServers: '+str(nregionservers))
        logging.info('Machines per tag: '+str(machines_per_tag))

        return machines_per_tag,regionTags



    def assignpertag(self,regions, nmachines):

        assignment = {}
        if nmachines > 0:
            for i in range(0,nmachines):
                assignment[i] = {}
                assignment[i]['load'] = 0
                assignment[i]['len'] = 0

            rmax = int(math.ceil(len(regions) / (nmachines+0.0)))

            #REGIONS ASSIGNMENT
            tmpmachines = copy.deepcopy(assignment.keys())


            while (len(regions)>0):

                region,req = regions.pop()

                binmostempty = (None,None)
                for machine in tmpmachines:
                    if binmostempty[0] == None:
                        binmostempty = (machine,assignment[machine]['load'])
                    if (binmostempty[1] > assignment[machine]['load']):
                        binmostempty = (machine,assignment[machine]['load'])
                if assignment[binmostempty[0]]['len'] < rmax:
                    assignment[binmostempty[0]][region]=req
                    assignment[binmostempty[0]]['load'] = assignment[binmostempty[0]]['load'] + req
                    assignment[binmostempty[0]]['len'] = assignment[binmostempty[0]]['len'] + 1
                else:
                    tmpmachines.remove(binmostempty[0])
                    regions.append((region,req))


        return assignment, regions


    #BINPACKING PROCEDURE
    def minimizemakespan(self,tag_to_machines,region_to_tag_reqs):
        readregions = []
        writeregions = []
        scanregions = []
        rwregions = []
        for region in region_to_tag_reqs:
            rtag = region_to_tag_reqs[region][0]
            rreqs = region_to_tag_reqs[region][1]
            if rtag == 'r':
                readregions.append((region,rreqs))
            elif rtag == 'w' :
                writeregions.append((region,rreqs))
            elif rtag == 's':
                scanregions.append((region,rreqs))
            elif rtag == 'rw':
                rwregions.append((region,rreqs))

        # If number of assgined machines is 0 for a specific tag but there are regions in that tag assign them to rw
        if(tag_to_machines['r']==0.0 and len(readregions)>0):
            rwregions = rwregions+readregions
            readregions = []
        if(tag_to_machines['w']==0.0 and len(writeregions)>0):
            rwregions = rwregions+writeregions
            writeregions = []
        if(tag_to_machines['s']==0.0 and len(scanregions)>0):
            rwregions = rwregions+scanregions
            scanregions = []

        readregions = sorted(readregions,key=lambda tupl: tupl[1])
        writeregions = sorted(writeregions,key=lambda tupl: tupl[1])
        scanregions = sorted(scanregions,key=lambda tupl: tupl[1])
        rwregions = sorted(rwregions,key=lambda tupl: tupl[1])

        nread = int(tag_to_machines['r'])
        nwrite = int(tag_to_machines['w'])
        nrw = int(tag_to_machines['rw'])
        nscan = int(tag_to_machines['s'])

        readmachines,readcopy = self.assignpertag(readregions,nread)
        writemachines, writecopy = self.assignpertag(writeregions,nwrite)
        scanmachines,scancopy = self.assignpertag(scanregions,nscan)
        rwmachines, rwcopy = self.assignpertag(rwregions,nrw)

        logging.info('ASSIGNMENT:')
        logging.info('read:'+str(readmachines))
        logging.info('write:'+str(writemachines))
        logging.info('scan:'+str(scanmachines))
        logging.info('rw:'+str(rwmachines))
        logging.info('LEFTOVERS:'+str(readcopy)+' '+str(writecopy)+' '+str(scancopy)+' '+str(rwcopy))

        return readmachines,writemachines,scanmachines,rwmachines


    #-----MINIMIZE MOVES------------------------------------
    def getClosest(self,regions,mtype,cur):
        res = None
        sim = 0
        for item in cur.keys():
            curtype = self._machine_type[item]
            if curtype == mtype:
                similar = 0
                for reg in cur[item].keys():
                    if reg in regions:
                        similar = similar + 1
                if similar > sim:
                    sim = similar
                    res = item
        return res


    def getPhysical(self,readmachines,writemachines,scanmachines,rwmachines):

        result = {}
        partialResult = {}
        partialResultConc = {}
        available_machines = self._stats.getRegionServers()
        newNMachines = len(available_machines)
        removeCheck = len(readmachines) + len(writemachines) + len(scanmachines) + len(rwmachines)
        logging.info('Machines considered in optimal distribution:'+str(removeCheck))

        creadmachines = copy.deepcopy(readmachines)
        cwritemachines = copy.deepcopy(writemachines)
        cscanmachines = copy.deepcopy(scanmachines)
        crwmachines = copy.deepcopy(rwmachines)

        cur = copy.deepcopy(self._current_config)

        if len(self._current_config)!=0:
            #NEED TO MINIMIZE MOVES

            newmachines = []
            for item in available_machines:
                if item not in self._machine_type.keys():
                    newmachines.append(item)
            logging.info('newmachines:'+str(newmachines))

            for item in readmachines.keys():
                physical = self.getClosest(readmachines[item],'r',cur)
                if not physical is None:
                    self._machine_type[physical] = 'r'
                    result[physical] = readmachines[item]
                    del creadmachines[item]
                    del cur[physical]

            for item in writemachines.keys():
                physical = self.getClosest(writemachines[item],'w',cur)
                if not physical is None:
                    self._machine_type[physical] = 'w'
                    result[physical] = writemachines[item]
                    del cwritemachines[item]
                    del cur[physical]

            for item in scanmachines.keys():
                physical = self.getClosest(scanmachines[item],'s',cur)
                if not physical is None:
                    self._machine_type[physical] = 's'
                    result[physical] = scanmachines[item]
                    del cscanmachines[item]
                    del cur[physical]

            for item in rwmachines.keys():
                physical = self.getClosest(rwmachines[item],'rw',cur)
                if not physical is None:
                    self._machine_type[physical] = 'rw'
                    result[physical] = rwmachines[item]
                    del crwmachines[item]
                    del cur[physical]

            #at this point every machine was matched to a possible assignment
            #next step is to check for missing assignments and possible change of configs
            machinesleft = cur.keys()+newmachines

            for item in creadmachines.keys():
                physical = machinesleft.pop()
                self._machine_type[physical] = 'r'
                self._actuator.configureServer(physical,'r', self._current_config.keys())
                result[physical] = readmachines[item]

            for item in cwritemachines.keys():
                physical = machinesleft.pop()
                self._machine_type[physical] = 'w'
                self._actuator.configureServer(physical,'w', self._current_config.keys())
                result[physical] = writemachines[item]

            for item in cscanmachines.keys():
                physical = machinesleft.pop()
                self._machine_type[physical] = 's'
                self._actuator.configureServer(physical,'s', self._current_config.keys())
                result[physical] = scanmachines[item]

            for item in crwmachines.keys():
                physical = machinesleft.pop()
                self._machine_type[physical] = 'rw'
                self._actuator.configureServer(physical,'rw', self._current_config.keys())
                result[physical] = rwmachines[item]


            #MOVE REGIONS INTO PLACE IF NEEDED
            self._actuator.distributeRegionsPerRS(result,self._machine_type,self._current_config)

            if len(self._current_config) > removeCheck:
                #FEWER MACHINES!
                while(self._actuator.isBusyCompactingFinal()):
                    logging.info('Waiting for major compact to finish in all regions before stopping any machine.')
                    time.sleep(20)
                assignedReg = result.keys()
                for regg in self._current_config.keys():
                    if regg not in assignedReg:
                        logging.info("Machine "+str(regg)+" going to be removed")
                        self._actuator.stopServer(regg)
                        self._actuator.tiramolaRemoveMachine(regg)


        else:
            #FIRST RECONFIGURATION
            logging.info('Current state empty. First reconfig.')


            for item in scanmachines.keys():
                physical = available_machines.pop()
                self._machine_type[physical] = 's'
                self._actuator.configureServer(physical,'s',available_machines)
                result[physical] = scanmachines[item]
                partialResult[physical] = scanmachines[item]
                self._actuator.distributeRegionsPerRS(partialResult,self._machine_type)
                partialResult = {}

            for item in readmachines.keys():
                physical = available_machines.pop()
                self._machine_type[physical] = 'r'
                self._actuator.configureServer(physical,'r',available_machines)
                result[physical] = readmachines[item]
                partialResult[physical] = readmachines[item]
                self._actuator.distributeRegionsPerRS(partialResult,self._machine_type)
                partialResult = {}


            for item in rwmachines.keys():
                physical = available_machines.pop()
                self._machine_type[physical] = 'rw'
                self._actuator.configureServer(physical,'rw',available_machines)
                result[physical] = rwmachines[item]
                partialResult[physical] = rwmachines[item]
                logging.info('partialResult:'+str(partialResult))
                self._actuator.distributeRegionsPerRS(partialResult,self._machine_type)
                partialResult = {}

            for item in writemachines.keys():
                physical = available_machines.pop()
                self._machine_type[physical] = 'w'
                self._actuator.configureServer(physical,'w',available_machines)
                result[physical] = writemachines[item]
                partialResult[physical] = writemachines[item]
                self._actuator.distributeRegionsPerRS(partialResult,self._machine_type)
                partialResult = {}


        logging.info('FINAL DISTRIBUTION:'+str(result))
        self._current_config = result
        return result


    #MAIN METHOD -----------------------------------------------------------------------------------------------------


    def cycle(self,bigbang,previousRegionStats):

        regionServerList = self._stats.getRegionServers()
        extraMachines = []
        actionNeeded = False
        machdying = 0
        nmach = 0
        dyingType = {}
        #check if any of the regionServers is dying
        for rsKey in regionServerList:
            dying = self.isRegionServerDying(self._stats.getRegionServerStats(rsKey))
            extra = self.isRegionServerExtra(self._stats.getRegionServerStats(rsKey))
            logging.info(rsKey+' '+" is dying? "+' '+str(dying))
            if dying:
                machdying = machdying + 1
                if self._machine_type.has_key(rsKey):
                    dyingType[self._machine_type[rsKey]]='True'
                actionNeeded = True
            if extra:
                extraMachines.append(rsKey)
            nmach = nmach + 1

        #CHECK IF WE NEED TO ADD/REMOVE MACHINES to address critical state
        if machdying/(nmach+0.0) >= self._CRITICAL_PERC or bigbang:
            #cluster in bad shape - add machines
            self._reconfigure = False


        #If we need to reconfigure stuff then:
        if actionNeeded and self._reconfigure:
            nregionservers = self._stats.getNumberRegionServers()
            regionStats = self._stats.getRegionStats()
            tagged_machines,tagged_regions = self.tagging(regionStats,previousRegionStats,nregionservers,dyingType)
            #going for ASSIGNMENT ALGORITHM
            readmachines,writemachines,scanmachines,rwmachines = self.minimizemakespan(tagged_machines,tagged_regions)
            #define which physical machine is going to accomodate which config (function 'f')
            self.getPhysical(readmachines,writemachines,scanmachines,rwmachines)
            self._reconfigure = False

        elif actionNeeded and not self._reconfigure:
            #CALL TIRAMOLA TO ADD OPENSTACK MACHINES
            logging.info('CALLING TIRAMOLA TO ADD MACHINES! number of machines:'+str(self._machtoadd))
            #for i in range(0,self._machtoadd):
            self._actuator.tiramolaAddMachine(self._machtoadd)
            #typesDying = len(dyingType)
            #self._machtoadd = max(self._machtoadd,typesDying)
            previousNOfSERVERS = self._stats.getNumberRegionServers()
            #NEED TO REFRESH STATS
            nregionservers = previousNOfSERVERS
            while(nregionservers!=previousNOfSERVERS+self._machtoadd):
                self._stats.refreshStats(False)
                nregionservers = self._stats.getNumberRegionServers()
            logging.info("New machines detected. Going for configs.")
            regionStats = self._stats.getRegionStats()
            #GOING FOR CONFIG WITH NEW MACHINES
            tagged_machines,tagged_regions = self.tagging(regionStats,previousRegionStats,nregionservers,dyingType)
            #going for ASSIGNMENT ALGORITHM
            readmachines,writemachines,scanmachines,rwmachines = self.minimizemakespan(tagged_machines,tagged_regions)
            #define which physical machine is going to accomodate which config (function 'f')
            self.getPhysical(readmachines,writemachines,scanmachines,rwmachines)


            #Update control vars; Fibonnacci sequence to add machines
            aux = self._machtoadd
            self._machtoadd = self._machtoadd + self._machtoaddBefore
            self._machtoaddBefore = aux
            self._reconfigure = False

        else:
            logging.info('Cluster is healthy.')
            if (len(extraMachines) > 0):
                #REMOVING INSTANCE
                previousNOfSERVERS = self._stats.getNumberRegionServers()
                #NEED TO REFRESH STATS

                nregionservers = self._stats.getNumberRegionServers() - 1
                regionStats = self._stats.getRegionStats()
                tagged_machines,tagged_regions = self.tagging(regionStats,previousRegionStats,nregionservers,dyingType)
                readmachines,writemachines,scanmachines,rwmachines = self.minimizemakespan(tagged_machines,tagged_regions)
                self.getPhysical(readmachines,writemachines,scanmachines,rwmachines)
                nregionservers = previousNOfSERVERS

                while(nregionservers==previousNOfSERVERS):
                    logging.info('Waiting for ganglia to forget machines.')
                    self._stats.refreshStats(False)
                    nregionservers = self._stats.getNumberRegionServers()

            self._machtoadd = 1
            self._machtoaddBefore = 1
            #self._reconfigure = True
            self._reconfigure = False #only reconf once

        #for reg in self._stats.getRegionServers():
        while(self._actuator.isBusyCompactingFinal()):
            logging.info('Waiting for major compact to finish in all regions.')
            time.sleep(20)

