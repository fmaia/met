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
import logging
import main_config
import time
import DecisionMaker
import Stats
import EC2Cluster,MonitorEC2
import sys

def main():

    #main loop on/off
    running = True
    #main loop number of runs
    runs = main_config.nloop
    ran = 0
    doStuff = False

    # cluster = EC2Cluster.EC2Cluster()
    # instances=cluster.describe_instances()
    # print str(instances)
    # images=cluster.describe_images()
    # print str(images)
    # cluster.run_instances("ami-c1aaabb5","ec2")
    #ec2Metrics= MonitorEC2.MonitorEC2([])
    #metrics = ec2Metrics.refreshMetrics()
    #print str(metrics)
    #sys.exit()

    #RAMP UP
    if main_config.rampup:
        logging.info("Now sleeping for ramp up time of 240s.")
        time.sleep(main_config.rampuptime)

    logging.info('MeT in business.')
    stats = Stats.Stats()
    decision_maker = DecisionMaker.DecisionMaker(stats)
    previousRegionStats = {}

    #Main loop
    while running:
        ran = ran + 1

        logging.info('Running cycle %s' % str(ran))

        stats.refreshStats(True)

        if ran == main_config.nsamples:
            doStuff = True
        else:
            if not doStuff:
                time.sleep(main_config.sleeptime)

        if doStuff:
            logging.info('Going to process cluster status.')

            decision_maker.cycle(False,previousRegionStats)

            doStuff = False
            ran = 0
            previousRegionStats = stats.getRegionStats()
            stats.resetStats()
            runs = runs - 1
            logging.info('Finished round.')
            time.sleep(main_config.sleeptime)



        if runs == 0:
            running = False

    logging.info('MeT ended and EXITED.')

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s',filename='met.log', level=logging.DEBUG)
    logging.info('Started MeT.')



    main()
