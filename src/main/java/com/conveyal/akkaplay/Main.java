package com.conveyal.akkaplay;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;

public class Main {
	
	static class GreeterCreator implements Creator<GreetingActor>{

		private String greeting;

		public GreeterCreator(String greeting) {
			this.greeting = greeting;
		}

		@Override
		public GreetingActor create() throws Exception {
			return new GreetingActor(greeting);
		}
		
	}

  public static void main(String[] args) {
	  ActorSystem system = ActorSystem.create("MySystem");
	  
	  Props greeterProps = Props.create(new GreeterCreator("bug off"));
	  ActorRef greeter = system.actorOf(greeterProps, "greeter");
	  
	  greeter.tell(new Greeting("Charlie Parker"), ActorRef.noSender());
	  //System.exit(0);
  }
}
