/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
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
import org.apache.zookeeper.data.Stat;

import com.module.zookeeper.EchoTest;

/**
 * 
 * A <a href="package.html">protocol to implement a distributed queue</a>.
 * 
 */

public class DistributedPriorityQueue {
    private static final Logger LOG = Logger.getLogger(DistributedPriorityQueue.class);
    private final String dir;

    private ZooKeeper zookeeper;
    private static List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private static Watcher watcher1;
    private final String prefix = "queue-";
    private DistributedPriorityQueue queue;
    private TreeMap<Long,String> orderedChildren;
    private LatchChildWatcher watcher;
    private int minPriority;

    public DistributedPriorityQueue(ZooKeeper zookeeper, String dir, List<ACL> acl){
        this.dir = dir;
	this.orderedChildren = new TreeMap<Long,String>();
	this.watcher = new LatchChildWatcher();

        if(acl != null){
            this.acl = acl;
        }
        this.zookeeper = zookeeper;
	this.minPriority = 999;
    }

    public DistributedPriorityQueue(ZooKeeper zookeeper, String dir, List<ACL> acl, int priorityLevels){
        this.dir = dir;
	this.orderedChildren = new TreeMap<Long,String>();
	this.watcher = new LatchChildWatcher();

        if(acl != null){
            this.acl = acl;
        }
        this.zookeeper = zookeeper;
	this.minPriority = priorityLevels;
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
		// TODO Auto-generated method stub
    	ZooKeeper zk = new ZooKeeper("localhost:2181", 15000, watcher1);
    	
		DistributedPriorityQueue queue = new DistributedPriorityQueue(zk, "/queue", acl);
	    queue.enqueue(new byte[] { (byte)0xe0, 0x4f, (byte)0xd0,
	    	    0x20, (byte)0xea, 0x3a, 0x69, 0x10, (byte)0xa2, (byte)0xd8, 0x08, 0x00, 0x2b,
	    	    0x30, 0x30, (byte)0x9d },200); //Enqueue element1 in the queue
	    queue.enqueue(new byte[] { (byte)0xba, (byte)0x8a, 0x0d,
	    	    0x45, 0x25, (byte)0xad, (byte)0xd0, 0x11, (byte)0x98, (byte)0xa8, 0x08, 0x00,
	    	    0x36, 0x1b, 0x11, 0x03 },201); // Enqueue element2 in the queue
	    System.out.println(queue.element()); // prints the head of the queue without modifying the queue
  //    queue.orderedChildren();  //Sets the Map of the children, ordered by id.
  //    queue.smallestChildName(); //Find the smallest child node.
        queue.remove(); // Removes first element from the queue
    }

    /**
     * Sets the Map of the children, ordered by id.
     */
    private void orderedChildren() throws KeeperException, InterruptedException {
	this.orderedChildren.clear();
        List<String> childNames = null;
        try{
            childNames = zookeeper.getChildren(dir, this.watcher);
        }catch (KeeperException.NoNodeException e){
            throw e;
        }

        for(String childName : childNames){
            try{
                //Check format
                if(!childName.regionMatches(0, prefix, 0, prefix.length())){
                	System.out.println("Found child node with improper name: " + childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = new Long(suffix);
                this.orderedChildren.put(childId,childName);
            }catch(NumberFormatException e){
            	LOG.warn("Found child node with improper format : " + childName + " " + e,e);
            }
       }
    }

    /**
     * Find the smallest child node.
     * @return The name of the smallest child node.
     */
    private String smallestChildName() throws KeeperException, InterruptedException {
        long minId = Long.MAX_VALUE;
        String minName = "";

        List<String> childNames = null;

        try{
            childNames = zookeeper.getChildren(dir, this.watcher);
        }catch(KeeperException.NoNodeException e){
            LOG.warn("Caught: " +e,e);
            return null;
        }

        for(String childName : childNames){
            try{
                //Check format
                if(!childName.regionMatches(0, prefix, 0, prefix.length())){
                	System.out.println("Found child node with improper name: " + childName);
                    continue;
                }
                 String suffix = childName.substring(prefix.length());
                  long childId = Long.parseLong(suffix);
                if(childId < minId){
                    minId = childId;
                    minName = childName;
                }
            }catch(NumberFormatException e){
                LOG.warn("Found child node with improper format : " + childName + " " + e,e);
            }
        }
 

        if(minId < Long.MAX_VALUE){
            return minName;
        }else{
            return null;
        }
    }

    /**
     * Return the head of the queue without modifying the queue.
     * @return the data at the head of the queue.
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] element() throws NoSuchElementException, KeeperException, InterruptedException {
        // element, take, and remove follow the same pattern.
        // We want to return the child node with the smallest sequence number.
        // Since other clients are remove()ing and take()ing nodes concurrently, 
        // the child with the smallest sequence number in orderedChildren might be gone by the time we check.
        // We don't call getChildren again until we have tried the rest of the nodes in sequence order.
        while(true){
            try{
                orderedChildren();
            }catch(KeeperException.NoNodeException e){
                throw new NoSuchElementException();
            }
            if(this.orderedChildren.size() == 0 ) throw new NoSuchElementException();

            for(String headNode : this.orderedChildren.values()){
                if(headNode != null){
                    try{
                        return zookeeper.getData(dir+"/"+headNode, false, null);
                    }catch(KeeperException.NoNodeException e){
                        //Another client removed the node first, try next
                    }
                }
            }

        }
    }


    /**
     * Attempts to remove the head of the queue and return it.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] remove() throws NoSuchElementException, KeeperException, InterruptedException {
        // Same as for element.  Should refactor this.
        while(true){
            try{
                orderedChildren();
            }catch(KeeperException.NoNodeException e){
                 throw new NoSuchElementException();
            }
            if(this.orderedChildren.size() == 0) throw new NoSuchElementException();

            for(String headNode : this.orderedChildren.values()){
                String path = dir +"/"+headNode;
                try{
                    byte[] data = zookeeper.getData(path, false, null);
                    zookeeper.delete(path, -1);
                    return data;
                }catch(KeeperException.NoNodeException e){
                    // Another client deleted the node first.
                }
            }

        }
    }

    private class LatchChildWatcher implements Watcher {

        CountDownLatch latch;

        public LatchChildWatcher(){
            latch = new CountDownLatch(1);
        }

        public void process(WatchedEvent event){
            LOG.debug("Watcher fired on path: " + event.getPath() + " state: " + 
                    event.getState() + " type " + event.getType());
            try{
                orderedChildren();
            }catch(KeeperException.NoNodeException e){
                throw new NoSuchElementException();
            }catch(Exception x){
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
     * Removes the head of the queue and returns it, blocks until it succeeds.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
     public byte[] take() throws KeeperException, InterruptedException {
        // Same as for element.  Should refactor this.
        while(true){
            try{
                orderedChildren();
            }catch(KeeperException.NoNodeException e){
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
                continue;
            }
            if(this.orderedChildren.size() == 0){
                this.watcher.await();
                continue;
            }

            for(String headNode : this.orderedChildren.values()){
                String path = dir +"/"+headNode;
                try{
                    byte[] data = zookeeper.getData(path, false, null);
                    zookeeper.delete(path, -1);
                    return data;
                }catch(KeeperException.NoNodeException e){
                    // Another client deleted the node first.
                }
            }
        }
    }

    /**
     * Inserts data into queue. Sets priority according to prio
     * @param data
     * @param prio
     * @return true if data was successfully added
     */
    private boolean enqueue(byte[] data, int prio) throws KeeperException, InterruptedException{
        for(;;){
            try{
                zookeeper.create(dir+"/"+prefix+prio, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            }catch(KeeperException.NoNodeException e){
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
            }
        }

    }

    /**
     * Inserts data into queue using enqueue(). Treats priority according to input data and sets the priority
     * parameter for using enqueue.
     * @param data
     * @return true if data was successfully added
     */
    public boolean offer(String data, List<String> priorityList) throws KeeperException, InterruptedException{
	int i;
	String aux;
	for (i=0; i<priorityList.size(); i++){
	    aux = priorityList.get(i);
	    if(data.indexOf(aux) >= 0){
		return enqueue(data.getBytes(), i);
	    }
	}
	return enqueue(data.getBytes(), this.minPriority);
    }

    /**
     * Returns the data at the first element of the queue, or null if the queue is empty.
     * @return data at the first element of the queue, or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] peek() throws KeeperException, InterruptedException{
        try{
            return element();
        }catch(NoSuchElementException e){
            return null;
        }
    }

      /**
     * Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
     * @return Head of the queue or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] poll() throws KeeperException, InterruptedException {
        try{
            return remove();
        }catch(NoSuchElementException e){
            return null;
        }
    }
 }