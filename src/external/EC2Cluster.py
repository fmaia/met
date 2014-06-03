'''
Copyright (c) 2013.

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

Based on:
Copyright (c) 2012
TIRAMOLA:  https://code.google.com/p/tiramola/
'''

import paramiko
import boto.ec2
import ec2_config
import logging
import sys, os, time

class EC2Cluster(object):
    '''
    This class holds all instances that take part in the virtual cluster.
    It can create and stop new instances.
    '''


    def __init__(self):
        #Actuator Parameters
        self._REGION = ec2_config.region
        self._ACCESSKEYID = ec2_config.access_key_id 
        self._SECRETACCESSKEY = ec2_config.secret_access_key 
        self._INSTANCETYPE = ec2_config.instance_type
        logging.info('EC2Cluster started.')
        
        
    def describe_instances(self, state=None, pattern=None):
        instances = []
        try:
            conn = boto.ec2.connect_to_region(self._REGION,aws_access_key_id=self._ACCESSKEYID,aws_secret_access_key=self._SECRETACCESSKEY)
        except Exception, ex:
            logging.error("Error getting connection "+str(ex))
        try:
            reservations = conn.get_all_instances()
        except Exception, ex:
            logging.warn(str(ex))
            
        members = ("id", "image_id", "public_dns_name", "private_dns_name",
    "state", "key_name", "ami_launch_index", "product_codes",
    "instance_type", "launch_time", "placement", "kernel",
    "ramdisk")

        for reservation in reservations:
            for instance in reservation.instances:
                details = {}
                for member in members:
                    val = getattr(instance, member, "")
                    # product_codes is a list
                    if val is None: val = ""
                    if hasattr(val, '__iter__'):
                        val = ','.join(val)
                    details[member] = val
                for var in details.keys():
                    exec "instance.%s=\"%s\"" % (var, details[var])
                if state:
                    if state == instance.state:
                        instances.append(instance)
                else:
                    instances.append(instance)
                        
        logging.debug("Instances:"+str(instances))
        ## if you are using patterns and state, show only matching state and id's
        matched_instances = []
        if pattern:
            for instance in instances:
                if instance.id.find(pattern) != -1:
                    matched_instances.append(instance)
                    
            if len(matched_instances) > 0:
                return matched_instances
            else:
                return None
        else:
            return instances

        
    def describe_images(self, pattern=None):
        all_ids = False
        owners = [ ]
        executable_by = [ ]
        image_ids = []

        if all_ids and (len(owners) or len(executable_by) or len(image_ids)):
            logging.warn("-a cannot be combined with owner, launch, or image list")
    
        conn = boto.ec2.connect_to_region(self._REGION,aws_access_key_id=self._ACCESSKEYID,aws_secret_access_key=self._SECRETACCESSKEY)
    
        if len(owners) == 0 and len(executable_by) == 0 and \
           len(image_ids) == 0 and not all_ids:
            try:
                owned = conn.get_all_images(image_ids=None,
                   owners=("self",), executable_by=None)
                launchable = conn.get_all_images(image_ids=None,
                   owners=None, executable_by=("self"))
                
                mylist = [ ]
                images = [ ]
                for image in owned:
                    mylist.append(image.id)
                    images.append(image)
                for image in launchable:
                    if image.id not in mylist:
                        images.append(image)
            except Exception, ex:
                print str(ex)
        else:
            try:
                images = conn.get_all_images(image_ids=image_ids, owners=owners, executable_by=executable_by)
            except Exception, ex:
                print str(ex)
            
#        print images
        
        ## if you are using patterns, show only matching names and emi's
        matched_images = []
        if pattern:
            for image in images:
                if image.location.find(pattern) != -1 and image.id.find("emi") != -1:
                    matched_images.append(image)
            if len(matched_images) > 0:
                return matched_images
            else:
                return None
        else:
            return images
        
    def read_user_data(self, user_data_filename):
        USER_DATA_CHUNK_SIZE = 512
        user_data = "";
        user_data_file = open(user_data_filename, "r")
        while 1:
            data = user_data_file.read(USER_DATA_CHUNK_SIZE)
            if not data:
                break
            user_data += data
        user_data_file.close()
        return user_data
    
    def run_instances(self, image_id=None,
        keyname=None,
        kernel_id=None,
        ramdisk_id=None,
        min_count=1,
        max_count=1,
        instance_type=None,
        group_names=['HBase'],
        user_data=None,
        user_data_file=None,
        addressing_type="public",
        zone="eu-west-1b"): 
    
        reservation = None
        if not instance_type:
            instance_type=self._INSTANCETYPE
        
        if image_id:
            if not user_data:
                if user_data_file:
                    user_data = self.read_user_data(user_data_file)
            conn = boto.ec2.connect_to_region(self._REGION,aws_access_key_id=self._ACCESSKEYID,aws_secret_access_key=self._SECRETACCESSKEY)
            try:
                reservation = conn.run_instances(image_id=image_id,
                                                  min_count=min_count,
                                                  max_count=max_count,
                                                  key_name=keyname,
                                                  security_groups=group_names,
                                                  user_data=user_data,
                                                  addressing_type=addressing_type,
                                                  instance_type=instance_type,
                                                  placement=zone,
                                                  kernel_id=kernel_id,
                                                  ramdisk_id=ramdisk_id)
            except Exception, ex:
                print(str(ex))
    
        else:
            print 'image_id must be specified'
            
#        print reservation.id
        instances = []
        
        ## add the newly run instances to the database
        members = ("id", "image_id", "public_dns_name", "private_dns_name",
        "state", "key_name", "ami_launch_index", "product_codes",
        "instance_type", "launch_time", "placement", "kernel",
        "ramdisk")
        
        for instance in reservation.instances:
            ## get instance details
            details = {}
            for member in members:
                val = getattr(instance, member, "")
                # product_codes is a list
                if val is None: val = ""
                if hasattr(val, '__iter__'):
                    val = ','.join(val)
                details[member] = val
            for var in details.keys():
                exec "instance.%s=\"%s\"" % (var, details[var])
                
            instances.append(instance)
                    
        return instances
        


        
    def terminate_instances(self, instance_ids):
            
        if (len(instance_ids) > 0):
            conn = boto.ec2.connect_to_region(self._REGION,aws_access_key_id=self._ACCESSKEYID,aws_secret_access_key=self._SECRETACCESSKEY)
            try:
                instances = conn.terminate_instances(instance_ids)
            except Exception, ex:
                print(str(ex))
                
            print "Terminating: ", instances
    
        else:
            print 'instance id(s) must be specified.'
            
    
#         
## Utilities
#   
    def block_until_running (self, instances):
        ''' Blocks until all defined instances have reached running state and an ip has been assigned'''
        ## Run describe instances until everyone is running
        tmpinstances = instances
        instances = []
        while len(tmpinstances) > 0 :
            time.sleep(30)
            print "Waiting for", len(tmpinstances), "instances."
            sys.stdout.flush()
            all_running_instances = self.describe_instances("running")
#            print all_running_instances
#            print tmpinstances
            for i in range(0,len(all_running_instances)):
                for j in range(0,len(tmpinstances)):
                    if (all_running_instances[i].id == tmpinstances[j].id) and (not (all_running_instances[i].public_dns_name == "0.0.0.0")):
                        tmpinstances.pop(j)
                        instances.append(all_running_instances[i])
                        break
        self.describe_instances()
        return instances
        
            
if __name__ == "__main__":
    ec2 = EC2Cluster()
    ec2.describe_instances("cluster")