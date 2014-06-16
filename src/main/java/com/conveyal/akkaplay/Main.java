package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;

import com.conveyal.akkaplay.actors.Executive;
import com.conveyal.akkaplay.actors.PrimeTester;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;

public class Main {
	


  public static void main(String[] args) throws IOException {
	  Config config = ConfigFactory.load();
	  String hostname = config.getString("akka.remote.netty.tcp.hostname");
	  int port = config.getInt("akka.remote.netty.tcp.port");
	  System.out.println( "running on "+hostname+":"+port );
	  String role = config.getString("role");
	  System.out.println( "role: "+role );
	  
	  ActorSystem system = ActorSystem.create("MySystem");
	  
	  if( role.equals("taskmaster") ){
		  System.out.println( "setting up master" );
		  ActorRef taskMaster = system.actorOf(Props.create(Executive.class));
		  
		  HttpServer server = HttpServer.create(new InetSocketAddress(8000), 5);
		  server.createContext("/", new StartPrimeSearchHandler(taskMaster, system));
		  server.setExecutor(null); // creates a default executor
		  server.start();
		  

	  } else {
		  Props greeterProps = Props.create(PrimeTester.class);
		  ActorRef taskMaster = system.actorOf(greeterProps, "tester");
		  System.out.println( "spinning up actor with path: "+taskMaster.path() );
	  }
	  
//	  long start = 10000000000000L;
//	  long span = 1000;
//	  for(long i=start; i<=start+span; i++){
//		  taskMaster.tell(new FindPrime(i), ActorRef.noSender());
//	  }
	  
  }
}
