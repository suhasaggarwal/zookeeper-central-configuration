package com.configuration.module;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;


/*
 * Allocating one persistent znode corresponding to each application server.
Configuration data particular to application,  in form of hash map , converted to byte array will be stored in each znode allocated for application server.
Application server will be able to get data from znode and initialise its configuration.

A sample hierarchy can be -

1)Configuration/app1
2)Configuration/app2
3)Configuration/app3
4)Configuration/app4

Each znode correpsonding to app server denoted by app1, app2, app3, app4 will hold configuration data in form of byte array.
Bean Property Key value pairs will be converted to hash map , which will be further converted to byte array and stored in designated znode for application.
In order to fetch  configuration data, application server can fetch the data from designated znode path allocated to it.
 
 */


public class ConfigurationAPIs {
	private static final Logger LOG = Logger.getLogger(ConfigurationAPIs.class);
	private final String dir;

	private ZooKeeper zookeeper;
	private static List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
	private static Watcher watcher1;
	private final String serverId="ServerId";
	private ConfigurationAPIs apis;
	private static TreeMap<String, String> orderedChildren;
	private LatchChildWatcher watcher;
	

	public ConfigurationAPIs(ZooKeeper zookeeper, String dir, List<ACL> acl) {
		this.dir = dir;
		this.orderedChildren = new TreeMap<String, String>();
		this.watcher = new LatchChildWatcher();

		if (acl != null) {
			this.acl = acl;
		}
		this.zookeeper = zookeeper;
		
	}

	
	
	public static void main(String[] args) throws InterruptedException,
			IOException, KeeperException, ClassNotFoundException {
		// TODO Auto-generated method stub
		ZooKeeper zk = new ZooKeeper("localhost:2181", 15000000, watcher1);
		
		//Populate Map with configuration Data
		Map<String, String> data = new HashMap<String, String>();
		data.put("ehCacheServerIP", "http://192.168.25.1651/ecacheServer/rest/");
		data.put("siteID", "6a35d3d14ebf2da12b99cf004fa2a1dd");
        data.put("SLoginRedirectUrl", "http://economictimes.indiatimes.com");
        
		// Parse Map to byte array
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(data);

		// Parse byte array to Map
		ByteArrayInputStream byteIn = new ByteArrayInputStream(
				byteOut.toByteArray());
		ObjectInputStream in = new ObjectInputStream(byteIn);
		Map<String, String> data2 = (Map<String, String>) in.readObject();
		// System.out.println(data2.toString());

		ConfigurationAPIs configuration = new ConfigurationAPIs(zk,
				"/configuration", acl);
		
		//Add configuration data corresponding to the application in Designated znode
		
		configuration.add(byteOut.toByteArray(), 1, "server1-"); // Add configuration data corresponding to server1 in central repository.
														
		configuration.add(byteOut.toByteArray(), 2, "server2-"); //Add configuration data corresponding to server2 in central repository.
						
		//Generate a map of all the znodes stored under configuration in Zookeeper Server
		configuration.orderedChildren();
		System.out.println("Znodes Ids corresponding to Application Servers");
		
		Set keys = orderedChildren.keySet();
		for (Iterator i = keys.iterator(); i.hasNext();) {
			String key = (String) i.next();
			String value = (String) orderedChildren.get(key);
			System.out.println(key + " = " + value);
		}
		
		//Fetch and Print configuration data corresponding to the znode allocated to a particular application.
		ByteArrayInputStream byteIn1 = new ByteArrayInputStream(configuration.get("server2-20000000001"));
		
		
		ObjectInputStream in1 = new ObjectInputStream(byteIn1);
		Map<Integer, String> data3 = (Map<Integer, String>) in1.readObject();
		System.out.println(data3.toString()); // 
		
		//Remove the znode corresponding to the application server
	//	configuration.remove("server2-20000000061");
	}

	/**
	 * Sets the Map of the children, ordered by id. All the Application Ids
	 * contained in application Server are stored in map as keys Example - Appid
	 * server1 = server1-10000000032
     * server2 = server2-20000000035
	 * value format - <Application Name, ZnodeId> 
	 * Application Name can be treated as Application Server Id - For example-
	 * server 1, server 2
	 */
	public void orderedChildren() throws KeeperException, InterruptedException {
		this.orderedChildren.clear();
		List<String> childNames = null;
		try {
			childNames = zookeeper.getChildren(dir, this.watcher);
		} catch (KeeperException.NoNodeException e) {
			throw e;
		}

		for (String childName : childNames) {
			
				String parts[] = childName.split("-");
				String suffix = parts[0];
				
				this.orderedChildren.put(suffix, childName);
			 
		}
	}

	
	/**
	 * Return the data contained in Application Znode - Takes Application Id as
	 * parameter
	 * @param  Takes Znode path corresponding to Application Server as Parameter
	 * @return the data in Application Znode
	 * @throws NoSuchElementException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public byte[] get(String path) throws NoSuchElementException,
			KeeperException, InterruptedException {
		
		
		while (true) {
			try {
				orderedChildren();
			} catch (KeeperException.NoNodeException e) {
				throw new NoSuchElementException();
			}
			if (this.orderedChildren.size() == 0)
				throw new NoSuchElementException();

			try {
				return zookeeper.getData(dir + "/" + path, false, null);
			} catch (KeeperException.NoNodeException e) {
				// Another client removed the node first, try next
			}

		}

	}

	/**
	 * Attempts to remove the znode corresponding to the application specified
	 * by passing application id as parameter
	 * @param Takes Znode path corresponding to Application Server as Parameter
	 * @return returns the data contained in znode specified by application id.
	 * @throws NoSuchElementException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public byte[] remove(String path) throws NoSuchElementException,
			KeeperException, InterruptedException {
		
		while (true) {
			try {
				orderedChildren();
			} catch (KeeperException.NoNodeException e) {
				throw new NoSuchElementException();
			}
			if (this.orderedChildren.size() == 0)
				throw new NoSuchElementException();

			try {
				byte[] data = zookeeper.getData(dir + "/" + path, false, null);
				zookeeper.delete(dir + "/" + path, -1);
				return data;
			} catch (KeeperException.NoNodeException e) {
				// Another client deleted the node first.
			}

		}
	}

	private class LatchChildWatcher implements Watcher {

		CountDownLatch latch;

		public LatchChildWatcher() {
			latch = new CountDownLatch(1);
		}

		public void process(WatchedEvent event) {
			LOG.debug("Watcher fired on path: " + event.getPath() + " state: "
					+ event.getState() + " type " + event.getType());
			try {
				orderedChildren();
			} catch (KeeperException.NoNodeException e) {
				throw new NoSuchElementException();
			} catch (Exception x) {
				x.printStackTrace();
				System.exit(1);
			}
			latch.countDown();
		}

		public void await() throws InterruptedException {
			latch.await();
		}
	}

	
	/**
	 * Inserts data into configuration.
	 * Takes AppName as parameter, add configuration data in znode corresponding to that App
	 * 
	 * @param data
	 * @param id
	 * @param appName
	 * @return true if data was successfully added
	 */
	private boolean add(byte[] data, int id, String appName) throws KeeperException,
			InterruptedException {
		for (;;) {
			try {
				zookeeper.create(dir + "/" + appName + id, data, acl,
						CreateMode.PERSISTENT_SEQUENTIAL);
				return true;
			} catch (KeeperException.NoNodeException e) {
				zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
			}
		}

	}

}