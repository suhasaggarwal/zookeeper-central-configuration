package com.module.zookeeper;

import com.module.zookeeper.EchoClient;
import com.module.zookeeper.EchoServer;

public class EchoTest {

	/**
	 * @param args
	 */
	
	
	public static void simpleLeadership() throws InterruptedException {
		 
		

		EchoClient client = new EchoClient(2181);
		
		
		EchoServer serverA = new EchoServer(5555, 2181);
		EchoServer serverB = new EchoServer(5556, 2181);
    	EchoServer serverC = new EchoServer(5557, 2181); 
		
		serverA.start();
		serverB.start();
		serverC.start(); 
	
		// Send messages
		for (int i = 0; i < 10; i++) {
		System.out.println("Client Sending:Hello-" + i);
		System.out.println(client.echo("Hello-" + i));
		}
		 
		if (serverA.isLeader()) {
		serverA.shutdown();
		}
		
		else if (serverB.isLeader()){
		serverB.shutdown();
		}
		
       else {
			serverC.shutdown();
		}
		 
		for (int i = 0; i < 10; i++) {
		System.out.println("Client Sending:Hello-" + i);
		System.out.println(client.echo("Hello-" + i));
		}
		
		 
		serverA.shutdown();
		serverB.shutdown();
		serverC.shutdown(); 
	   }

	
	
	
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		EchoTest.simpleLeadership();
	
	}

}
