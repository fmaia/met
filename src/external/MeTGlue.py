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

from py4j.java_gateway import JavaGateway
import time
import threading


class MeTGlue(object):

    DEFAULT_HBASE_JMX_PORT = 10102
    DEFAULT_SPLIT_TIMEOUT = 35 #seconds

    #HRegion
    REGION_READ_REQUEST_COUNT = 0
    REGION_WRITE_REQUEST_COUNT = 1
    REGION_TOTAL_SCANS_SIZE = 2

    #HRegionServer
    REGION_SERVER_READ_REQUEST_COUNT = 20
    REGION_SERVER_WRITE_REQUEST_COUNT = 21
    REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT = 22
    REGION_SERVER_BLOCK_CACHE_HIT_RATIO = 23
    REGION_SERVER_BLOCK_CACHE_HIT_CACHING_RATIO = 24
    REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX = 25
    REGION_SERVER_REQUESTS_PER_SECOND = 26
    REGION_SERVER_NUMBER_OF_ONLINE_REGIONS = 27
    REGION_SERVER_TOTAL_SCANS_SIZE = 28


    def __init__(self):
        print 'Initializing...'
        self.__gateway = JavaGateway()
        print 'JavaGateway created'
        self.__entry = self.__gateway.entry_point
        self.__entry.connect();
        print 'connected'
        self.__hbaseAdmin = self.__entry.getHBaseAdmin()
        self.__hbaseAdminLock = threading.Lock()
        print 'HBaseAdmin connection'
        self.__clusterStatus = self.__hbaseAdmin.getClusterStatus()
        print 'ClusterStatus'
        self.__splittingRegions = set()
        self.__splittingRegionsLock  = threading.Lock()


        print 'INITED'

    @property
    def hbaseAdmin(self):
        return self.__hbaseAdmin

    @property
    def clusterStatus(self):
        return self.__clusterStatus


    def getRegionServers(self,verbose=False):
        with self.__hbaseAdminLock:
            serverNames = self.__hbaseAdmin.getClusterStatus().getServers()
        return serverNames

    def getRegionServerStats(self,verbose=False):
        """
        Returns a dictionay of dictionarys with the region servers stats obtained through the JMX client.
        { regionServerName -> { REGION_SERVER_* : value} }
        """

        regionServerStats = {}
        with self.__hbaseAdminLock:
            serverNames = self.__hbaseAdmin.getClusterStatus().getServers()

        for serverName in serverNames.iterator():
            regionServerKey = serverName

            hostname = serverName.getHostname()
            port = serverName.getPort()
            if verbose:
                print 'Server ', serverName, 'host: ', hostname,  ' port: ', port

            rsStats = self.__entry.getRegionServerStats(hostname,self.DEFAULT_HBASE_JMX_PORT,True)

            regionServerStats[regionServerKey] = rsStats

            if verbose:
                print 'RegionServer: ', serverName
                print '\t REGION_SERVER_READ_REQUEST_COUNT: ', rsStats[self.REGION_SERVER_READ_REQUEST_COUNT]
                print '\t REGION_SERVER_WRITE_REQUEST_COUNT: ', rsStats[self.REGION_SERVER_WRITE_REQUEST_COUNT]
                print '\t REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT: ', rsStats[self.REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT]
                print '\t REGION_SERVER_BLOCK_CACHE_HIT_RATIO: ', rsStats[self.REGION_SERVER_BLOCK_CACHE_HIT_RATIO]
                print '\t REGION_SERVER_BLOCK_CACHE_HIT_CACHING_RATIO: ', rsStats[self.REGION_SERVER_BLOCK_CACHE_HIT_CACHING_RATIO]
                print '\t REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX: ', rsStats[self.REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX]
                print '\t REGION_SERVER_REQUESTS_PER_SECOND: ', rsStats[self.REGION_SERVER_REQUESTS_PER_SECOND]
                print '\t REGION_SERVER_NUMBER_OF_ONLINE_REGIONS: ', rsStats[self.REGION_SERVER_NUMBER_OF_ONLINE_REGIONS]
                print '\t REGION_SERVER_TOTAL_SCANS_SIZE: ', rsStats[self.REGION_SERVER_TOTAL_SCANS_SIZE]


        return regionServerStats


    def getRegionsPerServer(self,serverName,verbose=False):
        """
          Returns a list of the regions a region server currently owns
        """

        stats = []
        print 'getregionPerServer', serverName

        with self.__hbaseAdminLock:
            serverLoad = self.__hbaseAdmin.getClusterStatus().getLoad(serverName)

        #serverLoad = self.__clusterStatus.getLoad(serverName)
        print 'getregionPerServer2',serverLoad
        regionsLoad = serverLoad.getRegionsLoad()

        for region in regionsLoad.keys():
            stats.append(str(region))

        return stats

    def getRegionStats(self,verbose=False):
        """
        Returns a dictionay of dictionarys with the region stats obtained through the HBaseAdmin client.
        { regionName -> { REGION_READ_REQUEST_COUNT: value, REGION_WRITE_REQUEST_COUNT : value, REGION_TOTAL_SCANS_SIZE: value} }
        """

        stats = {}
        with self.__hbaseAdminLock:
            serverNames = self.__hbaseAdmin.getClusterStatus().getServers()

            for serverName in serverNames.iterator():

                serverLoad = self.__hbaseAdmin.getClusterStatus().getLoad(serverName)
                if verbose:
                    print 'Server ', serverName, ' addr: ', serverName.getHostAndPort(),  ' has ', serverLoad.getNumberOfRequests(), ' requests'

                regionsLoad = serverLoad.getRegionsLoad()

                for regionLoad in regionsLoad.keys():
                    regionKey = str(regionLoad)
                    regionData = { 'id': regionLoad }

                    rR = regionsLoad[regionLoad].getReadRequestsCount()
                    wR = regionsLoad[regionLoad].getWriteRequestsCount()
                    tSS = regionsLoad[regionLoad].getTotalScansSize()

                    regionData[self.REGION_READ_REQUEST_COUNT] = rR
                    regionData[self.REGION_WRITE_REQUEST_COUNT] = wR
                    regionData[self.REGION_TOTAL_SCANS_SIZE] = tSS

                    if verbose:
                        print 'Region: ', regionKey
                        print '\t readRequests: ', rR
                        print '\t writeRequests: ', wR
                        print '\t totalScansSize: ', tSS

                    stats[regionKey] = regionData

        return stats

    def getHServerLoadFromTableOrRegionName(self,tableNameOrRegionNameString):

        with self.__hbaseAdminLock:
            for serverName in self.__hbaseAdmin.getClusterStatus().getServers().iterator():
                serverLoad = self.__hbaseAdmin.getClusterStatus().getLoad(serverName)

                for regionLoad in serverLoad.getRegionsLoad().keys():
                    if tableNameOrRegionNameString == str(regionLoad):
                        break

        return serverName,serverLoad.getStorefileSizeInMB()


    def move(self,regionNameString,destServerNameString,verbose=True):
        """
        Moves the encodedRegionString to the destServerName.
        Both parameters should be strings, conversions are handled internally.
        """

        encodedRegionName = self.__entry.tobytes(regionNameString.split('.')[1])
        print 'DEBUG: ',encodedRegionName,' ',destServerNameString
        encodedServer = self.__entry.tobytes(str(destServerNameString))
        if verbose:
            print 'Moving. encodedRegion: ', encodedRegionName, ' destServerName: ', destServerNameString
        with self.__hbaseAdminLock:
            self.__hbaseAdmin.move(encodedRegionName,encodedServer)

    def split(self,tableNameOrRegionNameString,splitPointString,callback=None,verbose=True):
        """
        Splits the tableNameOrRegionNameString by the given splitPointString.
        If splitPointString is None, split regions in half.
        If specified callback is invoked upon split completion.
        Returns False if the region is already being split, True otherwise (implying the split command starts being executed).
        """

        with self.__splittingRegionsLock:

            if tableNameOrRegionNameString in self.__splittingRegions:
                if verbose:
                    print 'Region already being split. ', tableNameOrRegionNameString
                return False

            self.__splittingRegions.add(tableNameOrRegionNameString)

        if verbose:
            print 'Splitting. tableNameOrRegionName: ', tableNameOrRegionNameString, ' splitPoint: ', splitPointString



        if splitPointString:
            with self.__hbaseAdminLock:
                self.__hbaseAdmin.split(tableNameOrRegionNameString,splitPointString)
        else:
            with self.__hbaseAdminLock:
                self.__hbaseAdmin.split(tableNameOrRegionNameString)


        name,storeSize = self.getHServerLoadFromTableOrRegionName(tableNameOrRegionNameString)
        print 'serverName: ', name, ' storeFileSizeMB: ', storeSize
        sleepTime = storeSize * 90 / 1000. #heuristic ....

        t = threading.Thread(target=self.notifySplit,args=(tableNameOrRegionNameString,callback,sleepTime))
        t.start()

        return True


    def notifySplit(self,tableNameOrRegionNameString,callback,timeout):

        print 'waiting ....', self.__splittingRegions, ' for: ', timeout, ' seconds.'
        time.sleep(timeout)

        with self.__splittingRegionsLock:
            self.__splittingRegions.discard(tableNameOrRegionNameString)

        print 'wait done...', self.__splittingRegions

        if callback:
            print 'calling: ', callback
            callback(tableNameOrRegionNameString)


    def majorCompact(self,tableNameOrRegionNameString,verbose=True):
        if verbose:
            print 'Compacting: tableNameOrRegionName: ', tableNameOrRegionNameString
        with self.__hbaseAdminLock:
            self.__hbaseAdmin.majorCompact(tableNameOrRegionNameString)

    def testerCallback(tableNameOrRegionNameString):
        print 'Splitting done for: ', tableNameOrRegionNameString


