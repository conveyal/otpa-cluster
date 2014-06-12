package com.conveyal.akkaplay;

import java.util.Random;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.BoundedMessageQueueSemantics;
import akka.dispatch.RequiresMessageQueue;

public class PrimeTester extends UntypedActor implements RequiresMessageQueue<BoundedMessageQueueSemantics> {
	
	private Random rr;

	PrimeTester(){
		rr = new Random();
	}

  @Override
  public void onReceive(Object message) throws CosmicRayException {
	  if( message instanceof PrimeCandidate ){
		  if(rr.nextDouble()<0.01){
			  throw new CosmicRayException();
		  }
		  
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