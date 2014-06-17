package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.conveyal.akkaplay.actors.PrimeTester;
import com.conveyal.akkaplay.message.*;

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
import akka.routing.ActorSelectionRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class Executive extends UntypedActor {
	
	Router router;
	int tasksOut;
	private long timerStart=0;
	SupervisorStrategy strategy;
	int jobId=0;
	Map<Integer,ArrayList<WorkResult>> jobResults;
	
	ActorRef child;
	
	Executive(){
		jobResults = new HashMap<Integer,ArrayList<WorkResult>>();
		
		router = new Router(new RoundRobinRoutingLogic());
		
//		Function func = new Function<Throwable,Directive>(){
//			@Override
//			public Directive apply(Throwable t) throws Exception {
//				return SupervisorStrategy.restart();
//			}
//		};
//		strategy = new OneForOneStrategy(10,Duration.create("30 seconds"), func);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if( msg instanceof JobSpec ) {
			// if there are no workers to route to, bail
			if(router.routees().size()==0){
				getSender().tell(new JobId(-1), getSelf());
				return;
			}
			
			JobSpec jobSpec = (JobSpec)msg;
			jobSpec.jobId = jobId;
			
			jobResults.put(jobId, new ArrayList<WorkResult>());
			
			router.route( jobSpec, getSelf() );
	        
	        getSender().tell(new JobId(jobId), getSelf());
	        
	        jobId+=1;
		} else if( msg instanceof WorkResult) {
			WorkResult wr = (WorkResult)msg;
			
			jobResults.get(wr.jobId).add( wr );
			System.out.println( "prime:"+wr.num );
			
		} else if( msg instanceof JobResultQuery ){
			JobResultQuery jr = (JobResultQuery)msg;
			ArrayList<WorkResult> res = jobResults.get(jr.jobId);
			getSender().tell(new JobResult(res), getSelf());
		}  else if( msg instanceof AddManager) {
			AddManager aw = (AddManager)msg;
			System.out.println("add worker "+aw.remote);
			
			aw.remote.tell(new AssignExecutive(), getSelf());
			
			router = router.addRoutee( aw.remote );
		} else if( msg instanceof Terminated ) {
//			router = router.removeRoutee(((Terminated) msg).actor());
//			ActorRef r = getContext().actorOf(Props.create(Manager.class));
//		    getContext().watch(r);
//		    router = router.addRoutee(new ActorRefRoutee(r));
		}
	}
	

	
	@Override
	public SupervisorStrategy supervisorStrategy() {
	  return strategy;
	}

}
