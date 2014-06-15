package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;

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
		  HttpServer server = HttpServer.create(new InetSocketAddress(8000), 5);
		  server.createContext("/", new StartPrimeSearchHandler());
		  server.setExecutor(null); // creates a default executor
		  server.start();
		  
		  System.out.println( "prpare to delegate" );
		  ActorSelection remoteTaskMaster = system.actorSelection("akka.tcp://MySystem@127.0.0.1:2553/user/taskMaster");
		  System.out.println( remoteTaskMaster );
		  remoteTaskMaster.tell(new FindPrime(10000000000933L), ActorRef.noSender());
	  } else {
		  System.out.println( "starting up taskMaster to get some work done" );
		  Props greeterProps = Props.create(TaskMaster.class);
		  ActorRef taskMaster = system.actorOf(greeterProps, "taskMaster");
		  System.out.println( "spinning up actor with path: "+taskMaster.path() );
	  }
	  
//	  long start = 10000000000000L;
//	  long span = 1000;
//	  for(long i=start; i<=start+span; i++){
//		  taskMaster.tell(new FindPrime(i), ActorRef.noSender());
//	  }
	  
  }
}
