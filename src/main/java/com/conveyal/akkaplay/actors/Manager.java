package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;

import com.conveyal.akkaplay.message.AssignExecutive;
import com.conveyal.akkaplay.message.BuildGraph;
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

	private int curJobId = -1;
	private long jobSize = -1;
	private long jobsReturned = 0;
	private ArrayList<WorkResult> jobResults;
	private Router router;
	private ActorRef executive;
	private ActorRef graphBuilder;

	Manager() {

		ArrayList<Routee> routees = new ArrayList<Routee>();
		int cores = Runtime.getRuntime().availableProcessors();
		for (int i = 0; i < cores; i++) {
			ActorRef worker = getContext().actorOf(Props.create(PrimeTester.class), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);
		
		graphBuilder = getContext().actorOf(Props.create(GraphBuilder.class), "builder");

		System.out.println("starting manager with " + cores + " workers");
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof JobSpec) {
			JobSpec jobSpec = (JobSpec) message;
			System.out.println("got job bucket:" + jobSpec.bucket);
			
			//graphBuilder.tell(new BuildGraph(jobSpec.gtfs_path,jobSpec.osm_path), getSelf());
			
		} else if (message instanceof WorkResult) {
			WorkResult res = (WorkResult) message;

			jobsReturned += 1;

			if (res.isPrime) {
				System.out.println(res.num);
				jobResults.add(res);
				this.executive.forward(res, getContext());
			}

		} else if (message instanceof AssignExecutive) {
			this.executive = getSender();
			System.out.println("manager assigned to executive " + this.executive);
		} else if (message instanceof JobStatusQuery) {
			getSender().tell(new JobStatus(getSelf(), curJobId, jobsReturned / (float) jobSize), getSelf());
		}
	}

}
