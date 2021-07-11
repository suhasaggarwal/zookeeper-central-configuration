package com.configuration.module;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class LatchChildWatcher {

	    private static final Logger LOG = Logger.getLogger(LatchChildWatcher.class);
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
}



