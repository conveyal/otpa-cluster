package com.conveyal.akkaplay;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class TaskMaster extends UntypedActor {
	
	Router router;
	int tasksOut;
	private long timerStart=0;
	
	TaskMaster(){
		tasksOut = 0;
		
		List<Routee> routees = new ArrayList<Routee>();
		for (int i = 0; i < 50; i++) {
		      ActorRef r = getContext().actorOf(Props.create(PrimeTester.class));
		      getContext().watch(r);
		      routees.add(new ActorRefRoutee(r));
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if( msg instanceof FindPrime ){
			if(tasksOut==0){
				timerStart = System.currentTimeMillis();
			}
			
			tasksOut += 1;
			router.route(new PrimeCandidate(((FindPrime)msg).num), getSelf());
		} else if( msg instanceof WorkResult ){
			tasksOut -= 1;
			
			WorkResult res = (WorkResult)msg;
			if(res.isPrime)
				System.out.println(msg.toString());
			
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

}
