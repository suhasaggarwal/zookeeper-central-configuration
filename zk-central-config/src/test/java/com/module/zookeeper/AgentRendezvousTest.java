package com.module.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.module.zookeeper.Agent;
import com.module.zookeeper.SequentialZkNode;
import com.module.zookeeper.ZkUtils;

public class AgentRendezvousTest {
  
  private static final int ZK_PORT = 2181;
  
  private static ZkServer zkServer;
  
  private static final Logger LOG = Logger.getLogger(Agent.class);

  private final ZkClient zkClient=null;

  private int agentCount=0;

  private final String agentNumber=null;

  private final Object mutex = new Object();

  private final Random random = new Random();

  private SequentialZkNode agentRegistration;

  private final String meetingId=null;
  
  private static final int AGENT_COUNT = 8;

  private static final String MEETING_ID = "/M-debrief";
  
  @Before
  public static void setup() throws QuorumPeerConfig.ConfigException,
    IOException,
    KeeperException,
    InterruptedException {
 /*   zkServer = new ZkServer("/tmp/zkExample/data", "/tmp/zkExample/log", new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {}
    }, ZK_PORT);
    zkServer.start();
    
   */
	ZkClient zkClient = new ZkClient("localhost:" + ZK_PORT);
    zkClient.deleteRecursive(MEETING_ID);
    zkClient.create(MEETING_ID, new byte[0], CreateMode.PERSISTENT);
    zkClient.close();
  }

  @After
  public static void tearDown() throws InterruptedException {
    zkServer.shutdown();
  }
  
  @Test
  public static void agentRendezvous() throws InterruptedException {
    Agent agent[] = new Agent[AGENT_COUNT];
    
    for (int i = 0 ; i < AGENT_COUNT; i++) {
      agent[i] = new Agent("00" + i, MEETING_ID, AGENT_COUNT, ZK_PORT);
      agent[i].start();
    }

    for (int i = 0 ; i < AGENT_COUNT; i++) {
      agent[i].join();
    }    
  }

  
  public static void main(String[] args) throws InterruptedException, ConfigException, IOException, KeeperException {
		// TODO Auto-generated method stub

	  AgentRendezvousTest.setup();
	  AgentRendezvousTest.agentRendezvous();
	//  AgentRendezvousTest.tearDown();		
	}


  private void joinBriefing() throws InterruptedException {
	    Thread.sleep(random.nextInt(1000));
	    agentRegistration = ZkUtils.createEpheremalNode(meetingId, agentNumber, zkClient);
	    LOG.info("Agent:" + agentNumber + " joined Briefing:" + meetingId);

	    while (true) {
	      synchronized (mutex) {
	        List<String> list = zkClient.getChildren(meetingId);

	        if (list.size() < agentCount) {
	          LOG.info("Agent:" + agentNumber
	            + " waiting for other agents to join before presenting report...");
	          mutex.wait();
	        }
	        else {
	          break;
	        }
	      }
	    }
	  }

	  private void presentReport() throws InterruptedException {
	    LOG.info("Agent:" + agentNumber + " presenting report...");
	    Thread.sleep(random.nextInt(1000));
	    LOG.info("Agent:" + agentNumber + " completed report...");
//	    zkClient.delete(agentRegistration.getPath());
	  }

	  private void leaveBriefing() throws InterruptedException {

	    while (true) {
	      synchronized (mutex) {
	        List<String> list = zkClient.getChildren(meetingId);
	        if (list.size() > 0) {
	          LOG.info("Agent:" + agentNumber
	            + " waiting for other agents to complete their briefings...");
	          mutex.wait();
	        }
	        else {
	          break;
	        }
	      }
	    }
	    LOG.info("Agent:" + agentNumber + " left briefing");
	  }

}
