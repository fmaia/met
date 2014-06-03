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


'''

import boto.ec2.cloudwatch
import ec2_config
import sys
import logging

class MonitorEC2:
    def __init__(self, cluster):
    	self._REGION = ec2_config.region
        self._ACCESSKEYID = ec2_config.access_key_id 
        self._SECRETACCESSKEY = ec2_config.secret_access_key 
        self._INSTANCETYPE = ec2_config.instance_type
        self.cluster = cluster
        self.allmetrics={}

    def refreshMetrics(self):
        conn = boto.ec2.cloudwatch.connect_to_region(self._REGION,aws_access_key_id=self._ACCESSKEYID,aws_secret_access_key=self._SECRETACCESSKEY)
        self.allmetrics = conn.list_metrics()
        logging.debug(str(self.allmetrics))
        return self.allmetrics

