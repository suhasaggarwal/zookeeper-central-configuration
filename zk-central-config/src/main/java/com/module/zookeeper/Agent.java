/*  package com.welflex.zookeeper

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;

/**
 * Performs three simple tasks:
 * 1. Joins the meeting and waits for all attendees
 * 2. Presents
 * 3. Leaves after all attendees complete their presentations
 * 
 */
/*
public class Agent extends Thread implements IZkChildListener {
  private static final Logger LOG = Logger.getLogger(Agent.class);

  private final ZkClient zkClient;

  private final int agentCount;

  private final String agentNumber;

  private final Object mutex = new Object();

  private final Random random = new Random();

  private SequentialZkNode agentRegistration;

  private final String meetingId;
 
  public BufferedWriter out =null;
  
  
  
  
  
  public Agent(String agentNumber, String meetingId, int agentCount, int zkPort) {
    this.agentNumber = agentNumber;
    this.meetingId = meetingId;
    this.agentCount = agentCount;
    this.zkClient = new ZkClient("localhost:" + zkPort);
    zkClient.subscribeChildChanges(meetingId, this);
  }

  private void joinBriefing() throws InterruptedException {
    Thread.sleep(random.nextInt(1000));
    agentRegistration = ZkUtils.createEpheremalNode(meetingId, agentNumber, zkClient);
    LOG.info("Agent:" + agentNumber + " joined Briefing:" + meetingId);
    try {
		out.write("Agent:" + agentNumber + " joined Briefing:" + meetingId);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    try {
		out.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    while (true) {
      synchronized (mutex) {
        List<String> list = zkClient.getChildren(meetingId);

        if (list.size() < agentCount) {
          LOG.info("Agent:" + agentNumber
            + " waiting for other agents to join before presenting report...");
          try {
			out.write("Agent:" + agentNumber
			          + " waiting for other agents to join before presenting report...");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          try {
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
    try {
		out.write("Agent:" + agentNumber + " presenting report...");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    try {
		out.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    Thread.sleep(random.nextInt(1000));
    LOG.info("Agent:" + agentNumber + " completed report...");
    try {
		out.write("Agent:" + agentNumber + " completed report...");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    try {
		out.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    zkClient.delete(agentRegistration.getPath());
  }

  private void leaveBriefing() throws InterruptedException {

    while (true) {
      synchronized (mutex) {
        List<String> list = zkClient.getChildren(meetingId);
        if (list.size() > 0) {
          LOG.info("Agent:" + agentNumber
            + " waiting for other agents to complete their briefings...");
          try {
			out.write("Agent:" + agentNumber
			          + " waiting for other agents to complete their briefings...");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          try {
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          mutex.wait();
        }
        else {
          break;
        }
      }
    }
    LOG.info("Agent:" + agentNumber + " left briefing");
    try {
		out.write("Agent:" + agentNumber + " left briefing");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    try {
		out.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  
  
  
  
  
  
  
  
  
  
  
  

/*  public void run() {
    try {
      
    	URL url = null;
		try {
			url = new URL("http://localhost:8080/CallAgent");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	URLConnection conn=null;
		try {
			conn = url.openConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	conn.setDoOutput(true);
    	

    	try {
			BufferedWriter out = new BufferedWriter( new OutputStreamWriter( conn.getOutputStream() ) );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    	
     	
      joinBriefing();
      presentReport();
      leaveBriefing();
    }
*/
  
/* 
  
  @Override
  public void run() {
    try {
      joinBriefing();
      presentReport();
      leaveBriefing();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
    finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  } 
  
  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    synchronized (mutex) {
      mutex.notifyAll();
    }
  }
}
*/

package com.module.zookeeper;

import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

/**
 * Performs three simple tasks:
 * 1. Joins the meeting and waits for all attendees
 * 2. Presents
 * 3. Leaves after all attendees complete their presentations
 * 
 */
public class Agent extends Thread implements IZkChildListener {
  private static final Logger LOG = Logger.getLogger(Agent.class);

  private final ZkClient zkClient;

  private final int agentCount;

  private final String agentNumber;

  private final Object mutex = new Object();

  private final Random random = new Random();

  private SequentialZkNode agentRegistration;

  private final String meetingId;

  public Agent(String agentNumber, String meetingId, int agentCount, int zkPort) {
    this.agentNumber = agentNumber;
    this.meetingId = meetingId;
    this.agentCount = agentCount;
    this.zkClient = new ZkClient("localhost:" + zkPort);
    zkClient.subscribeChildChanges(meetingId, this);
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
   // zkClient.delete(agentRegistration.getPath());
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

  @Override
  public void run() {
    try {
      joinBriefing();
      presentReport();
      leaveBriefing();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
    finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    synchronized (mutex) {
      mutex.notifyAll();
    }
  }
}
