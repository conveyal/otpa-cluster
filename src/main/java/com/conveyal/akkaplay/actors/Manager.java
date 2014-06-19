package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;

import org.opentripplanner.routing.graph.Graph;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.akkaplay.message.AssignExecutive;
import com.conveyal.akkaplay.message.BuildGraph;
import com.conveyal.akkaplay.message.JobSpec;
import com.conveyal.akkaplay.message.JobStatus;
import com.conveyal.akkaplay.message.JobStatusQuery;
import com.conveyal.akkaplay.message.PrimeCandidate;
import com.conveyal.akkaplay.message.StartWorkers;
import com.conveyal.akkaplay.message.WorkResult;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class Manager extends UntypedActor {
	
	public enum Status {READY,BUILDING_GRAPH,WORKING};

	private int curJobId = -1;
	private long jobSize = -1;
	private long jobsReturned = 0;
	private ArrayList<WorkResult> jobResults;
	private Router router;
	private ActorRef executive;
	private ActorRef graphBuilder;
	
	private String curGraphId=null;
	private Graph graph=null;
	private Status status;

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
		status = Status.READY;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof JobSpec) {
			JobSpec jobSpec = (JobSpec) message;
			
			System.out.println("got job bucket:" + jobSpec.bucket);
			
			// if the current graph isn't the graph specified by the job, kick the graph builder into action
			if(graph==null || !curGraphId.equals(jobSpec.bucket)){
				curGraphId = jobSpec.bucket;
				graphBuilder.tell(new BuildGraph(jobSpec.bucket), getSelf());
				status = Status.BUILDING_GRAPH;
			} else {
				getSelf().tell(new StartWorkers(), getSelf());
			}
		} else if (message instanceof Graph){
			this.graph = (Graph)message;
			status = Status.READY;
			getSelf().tell(new StartWorkers(), getSelf());
		} else if (message instanceof StartWorkers){
			System.out.println( "set the workers doing their thing" );
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
