package com.conveyal.akkaplay.actors;

import com.conveyal.akkaplay.CosmicRayException;
import com.conveyal.akkaplay.message.*;

import akka.actor.UntypedActor;

public class PrimeTester extends UntypedActor {
	

	PrimeTester(){
	}

  @Override
  public void onReceive(Object message) throws CosmicRayException {
	  if( message instanceof PrimeCandidate ){
		  		  		  
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