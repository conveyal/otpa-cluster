package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;

import com.conveyal.akkaplay.message.AssignExecutive;
import com.conveyal.akkaplay.message.JobSpec;
import com.conveyal.akkaplay.message.JobStatus;
import com.conveyal.akkaplay.message.JobStatusQuery;
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
	
	private int curJobId=-1;
	private long jobSize=-1;
	private long jobsReturned=0;
	private ArrayList<WorkResult> jobResults;
	private Router router;
	private ActorRef executive;

	Manager(){
		
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
			
			curJobId = jobSpec.jobId;
			jobResults = new ArrayList<WorkResult>();
			jobSize = jobSpec.end-jobSpec.start;
			jobsReturned=0;
			
	        for(long i=jobSpec.start; i<jobSpec.end; i++){
	        	router.route(new PrimeCandidate(jobSpec.jobId, i), getSelf());
	        }
		} else if( message instanceof WorkResult ){
			WorkResult res = (WorkResult)message;
			
			jobsReturned += 1;
			
			if(res.isPrime) {
				System.out.println( res.num );
				jobResults.add( res );
				this.executive.forward(res, getContext());
			}
			
		} else if(message instanceof AssignExecutive){
			this.executive = getSender();
			System.out.println( "manager assigned to executive "+this.executive );
		} else if(message instanceof JobStatusQuery){
			getSender().tell( new JobStatus(curJobId, jobsReturned/(float)jobSize), getSelf() );
		}
	}

}
