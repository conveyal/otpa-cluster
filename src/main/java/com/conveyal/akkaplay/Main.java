package com.conveyal.akkaplay;

import java.net.URL;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;

public class Main {

  public static void main(String[] args) {
	  ActorSystem system = ActorSystem.create("MySystem");
	  
	  Props greeterProps = Props.create(TaskMaster.class);
	  ActorRef taskMaster = system.actorOf(greeterProps, "taskMaster");
	  
	  long start = 10000000000000L;
	  long span = 1000;
	  for(long i=start; i<=start+span; i++){
		  taskMaster.tell(new FindPrime(i), ActorRef.noSender());
	  }
	  
	  //greeter.tell(new Greeting("Charlie Parker"), ActorRef.noSender());
	  //System.exit(0);
  }
}
