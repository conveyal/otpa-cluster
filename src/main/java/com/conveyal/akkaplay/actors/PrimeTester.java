package com.conveyal.akkaplay.actors;

import java.util.Random;

import com.conveyal.akkaplay.CosmicRayException;
import com.conveyal.akkaplay.message.*;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.BoundedMessageQueueSemantics;
import akka.dispatch.RequiresMessageQueue;

public class PrimeTester extends UntypedActor {
	
	private Random rr;

	PrimeTester(){
		rr = new Random();
	}

  @Override
  public void onReceive(Object message) throws CosmicRayException {
	  if( message instanceof PrimeCandidate ){
//		  if(rr.nextDouble()<0.01){
//			  throw new CosmicRayException();
//		  }
		  
		  PrimeCandidate pc = (PrimeCandidate)message;
		  boolean ret = isPrime(pc.num);
		  getSender().tell(new WorkResult(pc.jobId,pc.num,ret), getSelf());
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