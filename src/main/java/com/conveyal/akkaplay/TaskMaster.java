package com.conveyal.akkaplay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.japi.Function;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class TaskMaster extends UntypedActor {
	
	Router router;
	int tasksOut;
	private long timerStart=0;
	SupervisorStrategy strategy;
	int jobId=0;
	Map<Integer,ArrayList<WorkResult>> jobResults;
	
	ActorRef child;
	
	TaskMaster(){
		tasksOut = 0;
		
		jobResults = new HashMap<Integer,ArrayList<WorkResult>>();
		
		List<Routee> routees = new ArrayList<Routee>();
		for (int i = 0; i < 50; i++) {
		      ActorRef r = getContext().actorOf(Props.create(PrimeTester.class), "primetester-"+i);
		      getContext().watch(r);
		      routees.add(new ActorRefRoutee(r));
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);
		
		Function func = new Function<Throwable,Directive>(){
			@Override
			public Directive apply(Throwable t) throws Exception {
				return SupervisorStrategy.restart();
			}
		};
		strategy = new OneForOneStrategy(10,Duration.create("30 seconds"), func);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if( msg instanceof FindPrime ){
			if(tasksOut==0){
				timerStart = System.currentTimeMillis();
			}
			
			tasksOut += 1;
			router.route(new PrimeCandidate(0, ((FindPrime)msg).num), getSelf());
		} else if( msg instanceof JobSpec ) {
			JobSpec jobSpec = (JobSpec)msg;
			
			jobResults.put(jobId, new ArrayList<WorkResult>());
			
	        for(long i=jobSpec.start; i<jobSpec.end; i++){
	        	router.route(new PrimeCandidate(jobId, i), getSelf());
	        }
	        
	        getSender().tell(new JobId(jobId), getSelf());
	        
	        jobId+=1;
		} else if( msg instanceof WorkResult ){
			tasksOut -= 1;
			
			WorkResult res = (WorkResult)msg;
			if(res.isPrime)
				System.out.println(msg.toString());
			
			jobResults.get(res.jobId).add( res );
			
			if( tasksOut==0 ){
				long dt = System.currentTimeMillis()-timerStart;
				System.out.println("All tasks back. It's been "+dt+"ms." );
			}
		} else if( msg instanceof Terminated ) {
			router = router.removeRoutee(((Terminated) msg).actor());
			ActorRef r = getContext().actorOf(Props.create(PrimeTester.class));
		    getContext().watch(r);
		    router = router.addRoutee(new ActorRefRoutee(r));
		}
	}
	

	
	@Override
	public SupervisorStrategy supervisorStrategy() {
	  return strategy;
	}

}
