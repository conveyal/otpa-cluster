package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;

import com.conveyal.akkaplay.message.AssignExecutive;
import com.conveyal.akkaplay.message.JobSpec;
import com.conveyal.akkaplay.message.PrimeCandidate;
import com.conveyal.akkaplay.message.WorkResult;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class Manager extends UntypedActor {
	
	private HashMap<Integer, ArrayList<WorkResult>> jobResults;
	private Router router;
	private ActorRef executive;

	Manager(){
		jobResults = new HashMap<Integer,ArrayList<WorkResult>>();
		
		ArrayList<Routee> routees = new ArrayList<Routee>();
		int cores = Runtime.getRuntime().availableProcessors();
		for(int i=0; i<cores; i++){
			ActorRef worker = getContext().actorOf(Props.create(PrimeTester.class), "worker-"+i);
			routees.add( new ActorRefRoutee( worker ) );
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);
		
		System.out.println( "starting manager with "+cores+" workers" );
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if( message instanceof JobSpec ){
			JobSpec jobSpec = (JobSpec)message;
			System.out.println( "got job "+jobSpec.start+"-"+jobSpec.end );
			
			jobResults.put(jobSpec.jobId, new ArrayList<WorkResult>());
			
	        for(long i=jobSpec.start; i<jobSpec.end; i++){
	        	router.route(new PrimeCandidate(jobSpec.jobId, i), getSelf());
	        }
		} else if( message instanceof WorkResult ){
			
			WorkResult res = (WorkResult)message;
			if(res.isPrime) {
				System.out.println( res.num );
				jobResults.get(res.jobId).add( res );
				this.executive.forward(res, getContext());
			}
			
		} else if(message instanceof AssignExecutive){
			this.executive = getSender();
			System.out.println( "manager assigned to executive "+this.executive );
		}
	}

}
