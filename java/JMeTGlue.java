/*
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
*/

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import py4j.GatewayServer;

import org.apache.hadoop.hbase.util.Bytes;


public class JMeTGlue {

	/* note that these constant must be kept in sync with the python code */
	public static final int REGION_SERVER_READ_REQUEST_COUNT = 20;
	public static final int REGION_SERVER_WRITE_REQUEST_COUNT = 21;
	public static final int REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT = 22;
	public static final int REGION_SERVER_BLOCK_CACHE_HIT_RATIO = 23;
	public static final int REGION_SERVER_BLOCK_CACHE_HIT_CACHING_RATIO = 24;
	public static final int REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX = 25;
	public static final int REGION_SERVER_REQUESTS_PER_SECOND = 26;
	public static final int REGION_SERVER_NUMBER_OF_ONLINE_REGIONS = 27;
	public static final int REGION_SERVER_TOTAL_SCANS_SIZE = 28;

	private Configuration config;
	private HBaseAdmin hbaseAdmin;
	private String ip;
	private int port;

	public JMeTGlue(String ip,int port){
		this.ip=ip;
		this.port=port;
	}

	public void connect()  throws MasterNotRunningException, ZooKeeperConnectionException 
	{
		config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.zookeeper.quorum", ip);
		config.set("hbase.zookeeper.property.clientPort", Integer.toString(port));

		hbaseAdmin = new HBaseAdmin(config);
	}

	public HBaseAdmin getHBaseAdmin(){
		return hbaseAdmin;
	}

	public static byte[] tobytes(String s){
		return Bytes.toBytes(s);
	}

	public static Map<Integer,Double> getRegionServerStats(String hostname,int port,boolean verbose) throws Exception {

		Map<Integer,Double> regionServerStats = new HashMap<Integer, Double>();

		String url = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
		
		System.out.println("Connecting to: " + url);
		
		
		
		JMXServiceURL serviceUrl = new JMXServiceURL(url);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
		try {
			MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
			
			Set<ObjectName> beanSet = mbeanConn.queryNames(new ObjectName("hadoop:service=RegionServer,name=RegionServerStatistics"), null);
			for(ObjectName obj : beanSet) {

				System.out.println("MBean is " + obj);

				double rRC = new Double((Long) mbeanConn.getAttribute(obj,"readRequestsCount"));
				double wRC = new Double((Long) mbeanConn.getAttribute(obj,"writeRequestsCount"));
				double bCEC = new Double((Long) mbeanConn.getAttribute(obj,"blockCacheEvictedCount"));
				double bCHR = new Double((Integer) mbeanConn.getAttribute(obj,"blockCacheHitRatio"));
				double bCHCR = new Double((Integer) mbeanConn.getAttribute(obj,"blockCacheHitCachingRatio"));
				double hBLI = new Double((Integer) mbeanConn.getAttribute(obj,"hdfsBlocksLocalityIndex"));
				double rPS = new Double((Float) mbeanConn.getAttribute(obj,"requests"));
				double nOR = new Double((Integer) mbeanConn.getAttribute(obj,"regions"));
				double tSS = new Double((Long) mbeanConn.getAttribute(obj,"totalScansSize"));

				regionServerStats.put(REGION_SERVER_READ_REQUEST_COUNT, rRC);
				regionServerStats.put(REGION_SERVER_WRITE_REQUEST_COUNT, wRC);
				regionServerStats.put(REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT, bCEC);
				regionServerStats.put(REGION_SERVER_BLOCK_CACHE_HIT_RATIO, bCHR);
				regionServerStats.put(REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX, hBLI);
				regionServerStats.put(REGION_SERVER_REQUESTS_PER_SECOND, rPS);
				regionServerStats.put(REGION_SERVER_NUMBER_OF_ONLINE_REGIONS, nOR);
				regionServerStats.put(REGION_SERVER_TOTAL_SCANS_SIZE, tSS);


				if (verbose) {
					System.out.println("readRequestsCount: " + rRC);
					System.out.println("writeRequestsCount: " + wRC);
					System.out.println("blockCacheEvictedCount: " + bCEC);
					System.out.println("blockCacheEvictedCount: " + bCHR);
					System.out.println("blockCacheHitCachingRatio: " + bCHCR);
					System.out.println("hdfsBlocksLocalityIndex: " + hBLI);
					System.out.println("requestsPerSecond: " + rPS);
					System.out.println("numberOfOnlineRegions: " + nOR);
					System.out.println("totalScansSize: " + tSS);
				}
			}
		} finally {
			jmxConnector.close();
		}


		return regionServerStats;

	}


	public static void main (String[] args) throws Exception{

		if (args != null && args.length < 2) {
			System.out.println("HBaseTuner ip port");
			System.exit(-1);
		}


		HBaseTuner hbTunner = new HBaseTuner(args[0],new Integer(args[1]));
		GatewayServer gatewayServer = new GatewayServer(hbTunner,25333);
		gatewayServer.start();

		System.out.println("Gateway Started");

	}
}
