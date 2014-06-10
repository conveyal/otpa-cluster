package com.conveyal.akkaplay;

import akka.actor.UntypedActor;

public class GreetingActor extends UntypedActor {
	
	private String greeting;

	GreetingActor(String greeting){
		this.greeting = greeting;
	}

  @Override
  public void onReceive(Object message) {
	  if( message instanceof Greeting ){
		  System.out.println(greeting+" "+((Greeting)message).who);
	  }
  }

}