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

Based on:
Copyright (c) 2012
TIRAMOLA:  https://code.google.com/p/tiramola/

'''

import socket
import xml.parsers.expat

class GParser:
    
    
    def __init__(self):
        self.inhost =0
        self.inmetric = 0
        self.allmetrics = {}
        self.currhostname=""

    def parse(self, file):
        p = xml.parsers.expat.ParserCreate()
        p.StartElementHandler = self.start_element
        p.EndElementHandler = self.end_element

        p.ParseFile(file)
        
#        print "inside function" , file
        if self.allmetrics == {}:
            raise Exception('Host/value not found')
        return self.allmetrics

    def start_element(self, name, attrs):
        if name == "HOST":
            self.allmetrics[attrs["NAME"]]={}
            self.inhost=1
            self.currhostname=attrs["NAME"]
        
        elif self.inhost==1 and name == "METRIC": 
            self.allmetrics[self.currhostname][attrs["NAME"]] = attrs["VAL"]

    def end_element(self, name):
            if name == "HOST" and self.inhost==1:
                self.inhost=0
                self.currhostname=""

class MonitorVms:
    def __init__(self, cluster):
        self.cluster = cluster
        
        self.ganglia_host = "10.0.108.3"
        self.ganglia_port = 8649

        self.allmetrics={};
        self.parser = GParser()
        


    def refreshMetrics(self):
        try:
            self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.soc.connect((self.ganglia_host,self.ganglia_port))
            self.allmetrics=self.parser.parse(self.soc.makefile("r"))
            self.soc.close()
        except socket.error:
            return self.allmetrics
        return self.allmetrics

