package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.conveyal.akkaplay.Pointset;
import com.conveyal.akkaplay.actors.SPTWorker;
import com.conveyal.akkaplay.message.*;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.japi.Function;
import akka.pattern.Patterns;
import akka.routing.ActorRefRoutee;
import akka.routing.ActorSelectionRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.util.Timeout;

public class Executive extends UntypedActor {

	int tasksOut;
	private long timerStart = 0;
	SupervisorStrategy strategy;
	int jobId = 0;
	Map<Integer, ArrayList<WorkResult>> jobResults;
	ArrayList<ActorSelection> managers;

	Executive() {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		managers = new ArrayList<ActorSelection>();

		// Function func = new Function<Throwable,Directive>(){
		// @Override
		// public Directive apply(Throwable t) throws Exception {
		// return SupervisorStrategy.restart();
		// }
		// };
		// strategy = new OneForOneStrategy(10,Duration.create("30 seconds"),
		// func);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof JobSpec) {
			// if there are no workers to route to, bail
			if (managers.size() == 0) {
				getSender().tell(new JobId(-1), getSelf());
				return;
			}

			// the executive gives jobs ids
			JobSpec jobSpec = (JobSpec) msg;
			jobSpec.jobId = jobId;

			// make a place to catch the results of the job
			jobResults.put(jobId, new ArrayList<WorkResult>());

			// send the job id to the client
			getSender().tell(new JobId(jobId), getSelf());
			
			// send the job to some managers
			//TODO send to a job manager
			managers.get(0).tell(jobSpec, getSelf());

			jobId += 1;
		} else if (msg instanceof WorkResult) {
			WorkResult wr = (WorkResult) msg;

			jobResults.get(wr.jobId).add(wr);
			System.out.println("prime:" + wr.num);

		} else if (msg instanceof JobResultQuery) {
			JobResultQuery jr = (JobResultQuery) msg;
			ArrayList<WorkResult> res = jobResults.get(jr.jobId);
			getSender().tell(new JobResult(res), getSelf());
		} else if (msg instanceof AddManager) {
			AddManager aw = (AddManager) msg;
			System.out.println("add worker " + aw.remote);

			aw.remote.tell(new AssignExecutive(), getSelf());

			managers.add(aw.remote);
		} else if (msg instanceof JobStatusQuery) {
			ArrayList<JobStatus> ret = new ArrayList<JobStatus>();
			for (ActorSelection manager : managers) {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(manager, new JobStatusQuery(), timeout);
				JobStatus result = (JobStatus) Await.result(future, timeout.duration());
				ret.add(result);
			}
			getSender().tell(ret, getSelf());
		} else if (msg instanceof Terminated) {
			// router = router.removeRoutee(((Terminated) msg).actor());
			// ActorRef r = getContext().actorOf(Props.create(Manager.class));
			// getContext().watch(r);
			// router = router.addRoutee(new ActorRefRoutee(r));
		}
	}

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

}
