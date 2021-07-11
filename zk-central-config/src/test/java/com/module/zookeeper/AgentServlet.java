package com.module.zookeeper;

import java.io.IOException;

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
public final class AgentServlet extends HttpServlet {

public void doGet(HttpServletRequest request,HttpServletResponse response)
  throws IOException, ServletException {		 
	
	try {
		AgentRendezvousTest.setup();
	} catch (ConfigException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (KeeperException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		
		
	}
	try {
		AgentRendezvousTest.agentRendezvous();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}

}