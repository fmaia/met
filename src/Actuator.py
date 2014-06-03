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
import actuator_config
import os
import paramiko
import time
import OpenStackCluster
import EC2Cluster
import operator
from Queue import Queue
from threading import Thread


class Actuator(object):

    def __init__(self,stats):
        self._metglue = stats.getMeTGlue()
        self._stats = stats
        #Actuator Parameters
        self._TEMPLATE = actuator_config.template
        self._TARGET = actuator_config.target
        self._WHERETO = actuator_config.whereto
        self._USERNAME = actuator_config.username
        self._PASSWORD = actuator_config.password
        self._MASTER = actuator_config.master
        self._CLUSTERTYPE= actuator_config.clustertype
        # Cluster implementation 
        if self._CLUSTERTYPE == "EC2":
            self._eucacluster = EC2Cluster.EC2Cluster()
            logging.info("Created EC2 cluster")
        elif self._CLUSTERTYPE == "OS":
                self._eucacluster = OpenStackCluster.OpenStackCluster()
                logging.info("Created OpenStackCluster")
        else: 
            logging.warn("Invalid cluster type "+self._CLUSTERTYPE)
        #queue for major compaction
        self.queue = Queue()
        self.queuePending = Queue()
        #Three threads for major compacting meaning three simultaneous major compacts in the cluster
        for i in range(0, 3):
            thread = Thread(target=self.major_compact, args=(i,self.queue,self.queuePending,))
            thread.setDaemon(True)
            thread.start()
        logging.info('Actuator started.')

    def copyToServer(self,host,whereto,filepath):
        logging.info("Copying files to "+ str(host))
        tries=0
        while tries<100:
            try:
                tries+=1
                transport = paramiko.Transport((host, 22))
                transport.connect(username = self._USERNAME, password = self._PASSWORD)
                transport.open_channel("session", host, "localhost")
                sftp = paramiko.SFTPClient.from_transport(transport)
                splittedpath = filepath.split('/')[-1]
                sftp.put(filepath, whereto+'/'+splittedpath)
                sftp.close()
                logging.info('File '+str(filepath)+' copied to '+str(host)+'.')
                break
            except:
                print ("Unable to connect to node  " + str(host)+ " after "+str(tries)+" attempts.")
                time.sleep(5)




    def configFile(self,template,final,block,memu,meml):
        os.system("sed 's/BLOCKCACHESIZE/"+str(block)+"/g; s/GLOBALMEMSTOTEUPPERLIMIT/"+str(memu)+"/g; s/GLOBALMEMSTORELOWERLIMT/"+str(meml)+"/g' " + template + " > " + final)
        print 'File ',template,' configured with block:',str(block),' memu:',str(memu),' meml:',str(meml)

    def isBusyCompactingFinal(self):
        if self.queuePending.empty():
            return False
        else:
            return True

    def isBusyCompacting(self,server):
        x = os.popen("curl \"http://"+server+":60030/rs-status\"").read()
        return "RUNNING" in x

    def isBusy(self):
        x = os.popen("curl \"http://"+self._MASTER+":60010/master-status\"").read()
        return not "No regions in transition." in x

    def isAlive(self,rserver):
        res = False
        servername = str(rserver)
        alive = self._metglue.getRegionServers()
        logging.info('checking server '+str(servername)+'alive servers:'+str(alive))
        for sal in alive.iterator():
            if str(sal).startswith(servername):
                res = True
        return res


    def restartServer(self,host):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(host, username=self._USERNAME, password=self._PASSWORD)
        except:
            logging.info("Unable to connect to node  " + str(host))

        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh stop regionserver')
        logging.info(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh start regionserver')
        logging.info(str(stdout.readlines()))
        ssh.close()
        logging.info('Server '+str(host)+' restarted ('+str(stdout)+').')

    def stopServer(self,host):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(host, username=self._USERNAME, password=self._PASSWORD)
        except:
            logging.info("Unable to connect to node  " + str(host))

        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh stop regionserver')
        logging.info(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor stop')
        logging.info(str(stdout.readlines()))
        ssh.close()
        logging.info('Server '+str(host)+' stopped ('+str(stdout)+').')

    def configureServer(self,server,servertag,available_machines=None):
        #SERVER CONFIGURATION
        if servertag=='r':
            self.configFile(self._TEMPLATE,self._TARGET,0.55,0.1,0.07)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='w':
            self.configFile(self._TEMPLATE,self._TARGET,0.10,0.55,0.5)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='s':
            self.configFile(self._TEMPLATE,self._TARGET,0.55,0.1,0.07)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='rw':
            self.configFile(self._TEMPLATE,self._TARGET,0.45,0.20,0.15)
            self.copyToServer(server,self._WHERETO,self._TARGET)

        #moving regions in the RS to other place before restart
        theseRegions = self._metglue.getRegionsPerServer(self._stats.getServerLongName(server))
        temporaryHolder = None

        serverlist = available_machines
        i=0

        for regionn in theseRegions:
            if len(serverlist) > 0 :
                try:
                    temh = serverlist[i%len(serverlist)]
                    if temh != server:
                        temporaryHolder = temh
                        i += 1
                    else:
                        i += 1
                        temh = serverlist[i%len(serverlist)]
                        temporaryHolder = temh
                        i += 1

                    if not regionn.startswith('-ROOT') and not regionn.startswith('.META'):
                        self._metglue.move(regionn,self._stats.getServerLongName(temporaryHolder),False)

                except Exception, err:
                    logging.error('ERROR:'+str(err))
                logging.info('Temporarily moving region '+str(regionn)+' to '+str(temporaryHolder) +'.')
            else:
                #the case when is the last regionserver
                try:
                    temh = self._stats.getRegionServers()[i%self._stats.getNumberRegionServers()]
                    if temh != server:
                        temporaryHolder = temh
                        i += 1
                    else:
                        i += 1
                        temh = self._stats.getRegionServers()[i%self._stats.getNumberRegionServers()]
                        temporaryHolder = temh
                        i += 1
                    if not regionn.startswith('-ROOT') and not regionn.startswith('.META'):
                        self._metglue.move(regionn,self._stats.getServerLongName(temporaryHolder),False)

                except Exception, err:
                    logging.error('ERROR:'+str(err))
                logging.info('Temporarily moving region '+str(regionn)+' to '+str(temporaryHolder) +'.')

        #check if we can restart
        while(self.isBusy()):
            time.sleep(2)

        #GOING FOR RESTART
        self.restartServer(server)

        while(not self.isAlive(server)):
            logging.info('Waiting for server ' + str(server) + ' to wake up.')
            time.sleep(2)





    #Distribute (move) regions to regionservers
    def distributeRegionsPerRS(self,machines_to_regions=None,machine_type=None,current_config={}):
        self._stats.refreshStats(False)
        longServerNames = self._stats.getServerLongNames()
        logging.info('distributeRegionsPerRS: '+str(machines_to_regions))

        logging.info('machine_to_regions:'+str(machines_to_regions))
        #NOT FIRST RECONFIGURATION
        if current_config != {}:

            for machine in machines_to_regions.keys():
                if machine in current_config.keys():
                    newregions = {}
                    regions = machines_to_regions[machine]
                    for nregion in regions:
                        if nregion not in current_config[machine]:
                            #newregions.append(nregion)
                            newregions.update({nregion:regions[nregion]})
                    machines_to_regions[machine] = newregions
                    logging.info('machines_to_regions after current_config'+str(machines_to_regions[machine]))


            #MOVING REGIONS INTO PLACE
            for rserver in machines_to_regions:
                for region in machines_to_regions[rserver]:
                    if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
                        ser = longServerNames[rserver]
                        try:
                            self._metglue.move(region,ser,False)
                            while(self.isBusy()):
                                time.sleep(2)
                            if region not in self._metglue.getRegionsPerServer(ser):
                                self._metglue.move(region,ser,False)
                                while(self.isBusy()):
                                    time.sleep(2)
                            logging.info('Moving region '+ str(region)+ ' to '+ str(ser)+ ' DONE.')
                        except Exception, err:
                            logging.error('ERROR:'+str(err))


            for regions_to_move in machines_to_regions:
                logging.info('Regions_to_move: '+str(regions_to_move))
                reglist = copy.deepcopy(machines_to_regions[regions_to_move])
                resultl = {}
                resultl[regions_to_move] = reglist
                resultl['machine_type'] = machine_type
                logging.info('Putting in queue:')
                self.queue.put(resultl)
                self.queuePending.put(True)

        #FIRST RECONFIGURATION
        else:
             #MOVING REGIONS INTO PLACE
            for rserver in machines_to_regions:
                for region in machines_to_regions[rserver]:
                    if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
                        ser = longServerNames[rserver]
                        try:
                            self._metglue.move(region,ser,False)
                            while(self.isBusy()):
                                logging.info('waiting on move to finish')
                                time.sleep(2)
                            logging.info('Moving region '+ str(region)+ ' to '+ str(ser)+ ' DONE.')
                            if region not in self._metglue.getRegionsPerServer(ser):
                                self._metglue.move(region,ser,False)
                                while(self.isBusy()):
                                    time.sleep(2)
                            logging.info('Moving region '+ str(region)+ ' to '+ str(ser)+ ' DONE.')
                        except Exception, err:
                            logging.error('ERROR:'+str(err))



            logging.info('machine_type:'+str(machine_type))
            machines_to_regions.update({'machine_type':machine_type})
            logging.info('machine_to_regions2:'+str(machines_to_regions))
            logging.info('Putting in queue:')
            self.queue.put(machines_to_regions)
            self.queuePending.put(True)


    #REMOVE MACHINE
    def tiramolaRemoveMachine(self,name):
        instances = self._eucacluster.describe_instances()
        id = []
        for instance in instances:
            if instance.name == name:
                id.append(instance.id)

        self._eucacluster.terminate_instances(id)
        logging.info("Removed instance "+str(name)+" with id:"+str(id))

    #ADD MACHINE
    def tiramolaAddMachine(self, machtoadd):

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        instances = self._eucacluster.describe_instances()
        maxID=0
        for instance in instances:
            if (instance.name.startswith("region")):
                num=int(instance.name[6:])
                if num > maxID:
                    maxID=num
        instances = []
        for i in range(0,machtoadd):
            name="region"+str(maxID+1)
            maxID = maxID + 1
            instances.extend(self._eucacluster.run_instances(" ",name))
        logging.info("Launched new instance(s): " + str(instances))
        mInstances = self._eucacluster.block_until_running(instances)
        for instance in mInstances:
            hosts = open('/tmp/hosts', 'a')
            try:
                ssh.connect(instance.public_dns_name, username=self._USERNAME, password=self._PASSWORD)
            except:
                logging.error("Unable to connect to node  " + str(instance.public_dns_name))

            stdin, stdout, stderr = ssh.exec_command('echo \"'+instance.name+"\" > /etc/hostname")
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('hostname \"'+instance.name+"\"")
            logging.info(str(stdout.readlines()))
            hosts.write(instance.private_dns_name + "\t" + instance.name +"\n")

        hosts.close()

        nins = []
        for ins in mInstances:
            nins.append(ins.public_dns_name)
        lista = ["master","10.0.108.16","10.0.108.19","10.0.108.11"]

        rss = []
        instances = self._eucacluster.describe_instances()
        for rs in instances:
            if (rs.name.startswith("region")):
                rss.append(rs.public_dns_name)

        lista.extend(rss)

        for node in lista:
            transport = paramiko.Transport((node, 22))
            try:
                transport.connect(username = 'root', password = '123456')
            except:
                logging.error("Unable to connect to node  " + str(node))
            transport.open_channel("session", node, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info("Sending /etc/hosts to node:  " + str(node))
            sftp.put( "/tmp/hosts", "/etc/hosts")
            sftp.close()

        os.system("echo '"+self._PASSWORD+"' |sudo -S cp /tmp/hosts /etc/hosts")

        for instance in mInstances:
            try:
                ssh.connect(instance.public_dns_name, username='root', password='123456')
            except:
                logging.error("Unable to connect to node  " + str(instance.public_dns_name))

            #START GANGLIA ON ADDED MACHINE
            stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor start')
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/hadoop-daemon.sh start datanode')
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh start regionserver')
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
            logging.info(str(stdout.readlines()))
            ssh.close()



    def major_compact(self,i,queue,queuePending):
        logging.info('Thread '+str(i) +'to compact created!')

        while True:
            toCompact = queue.get(True,None)
            logging.info('Thread '+str(i) +' is starting again to compact!')

            time.sleep(2)
            self._stats.refreshStats(False)

            logging.info('Thread getting from queue:'+str(toCompact))
            machine_type = toCompact.pop('machine_type')
            machines_to_regions = toCompact

            for rserver in machines_to_regions:
                rserver_stats = self._stats.getRegionServerStats(rserver)
                locality = rserver_stats['hbase.regionserver.hdfsBlocksLocalityIndex']
                logging.info('Server '+str(rserver)+' has locality of:'+str(locality))

                if (int(locality) < 70 and machine_type[rserver]=="w") or (int(locality) < 90 and machine_type[rserver]!="w"):
                    #major_compact first the hotspot regions
                    sorted_regions = sorted(machines_to_regions[rserver].iteritems(), key=operator.itemgetter(1))

                    for a in sorted_regions:
                        region = a[0]
                        time.sleep(1)
                        self._stats.refreshStats(False)
                        rserver_stats = self._stats.getRegionServerStats(rserver)
                        locality = rserver_stats['hbase.regionserver.hdfsBlocksLocalityIndex']
                        if (int(locality) < 95):
                            if not region.startswith('load') and not region.startswith('len'):
                                try:
                                    logging.info('Major compact of: '+str(region)+' with locality: '+str(locality))
                                    self._metglue.majorCompact(region)
                                    while(self.isBusyCompacting(rserver)):
                                        logging.info('Waiting for major compact to finish in '+str(rserver)+'...')
                                        time.sleep(20)
                                except Exception, err:
                                    logging.error('ERROR:'+str(err))
            queuePending.get(True,None)
            queue.task_done()
            queuePending.task_done()


