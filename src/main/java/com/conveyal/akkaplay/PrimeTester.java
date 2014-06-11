package com.conveyal.akkaplay;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.BoundedMessageQueueSemantics;
import akka.dispatch.RequiresMessageQueue;

public class PrimeTester extends UntypedActor implements RequiresMessageQueue<BoundedMessageQueueSemantics> {
	
	PrimeTester(){
	}

  @Override
  public void onReceive(Object message) {
	  if( message instanceof PrimeCandidate ){
		  long payload = ((PrimeCandidate)message).num;
		  boolean ret = isPrime(payload);
		  getSender().tell(new WorkResult(payload,ret), getSelf());
	  } else {
		  unhandled(message);
	  }
  }

private boolean isPrime(long num) {
	double top = Math.sqrt(num);
	for(int i=2; i<=top; i++){
		if(num%i==0){
			return false;
		}
	}
	return true;
}

}