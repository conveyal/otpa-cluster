package com.conveyal.otpac.actors;

import java.util.ArrayList;
import java.util.HashMap;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.graph.Graph;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.conveyal.otpac.message.AssignExecutive;
import com.conveyal.otpac.message.BuildGraph;
import com.conveyal.otpac.message.JobSliceDone;
import com.conveyal.otpac.message.JobSliceSpec;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.PrimeCandidate;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.StartWorkers;
import com.conveyal.otpac.message.WorkResult;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.util.Timeout;

public class Manager extends UntypedActor {
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public enum Status {READY,BUILDING_GRAPH,WORKING};

	private int curJobId = -1;
	private long jobSize = -1;
	private long jobsReturned = 0;
	private ArrayList<WorkResult> jobResults;
	private ArrayList<ActorRef> workers;
	private Router router;
	private ActorRef jobManager;
	private ActorRef graphBuilder;
	
	private String curGraphId=null;
	private Graph graph=null;
	private Status status;
	
	AmazonS3 s3;
	private JobSliceSpec jobSpec=null;

	Manager() {
		// grab credentials from "~.aws/credentials"
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials();
		s3 = new AmazonS3Client(creds);

		ArrayList<Routee> routees = new ArrayList<Routee>();
		workers = new ArrayList<ActorRef>();
		int cores = Runtime.getRuntime().availableProcessors();
		for (int i = 0; i < cores; i++) {
			ActorRef worker = getContext().actorOf(Props.create(SPTWorker.class), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
			workers.add( worker );
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);
		
		graphBuilder = getContext().actorOf(Props.create(GraphBuilder.class), "builder");

		System.out.println("starting manager with " + cores + " workers");
		status = Status.READY;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof JobSliceSpec) {
			onMsgJobSliceSpec((JobSliceSpec) message);
		} else if (message instanceof Graph){
			onMsgGetGraph((Graph) message);
		} else if (message instanceof StartWorkers){
			onMsgStartWorkers();
		} else if (message instanceof WorkResult) {
			onMsgWorkResult((WorkResult) message);
		} else if (message instanceof JobStatusQuery) {
			getSender().tell(new JobStatus(getSelf(), curJobId, jobsReturned / (float) jobSize), getSelf());
		} else {
			unhandled(message);
		}
	}

	private void onMsgWorkResult(WorkResult res) {
		jobsReturned += 1;
		log.debug("got: {}", res);
		log.debug("{}/{} jobs returned", jobsReturned, jobSize);
		
		if(res.success){
			jobManager.forward(res, getContext());;
		}
		
		if(jobsReturned==jobSize){
			jobManager.tell(new JobSliceDone(), getSelf());
		}
	}

	private void onMsgStartWorkers() throws Exception {
		log.debug( "set the workers doing their thing" );
		
		SampleSet sampleSet = new SampleSet(this.jobSpec.to, this.graph.getSampleFactory());
		
		//send graph to all workers
		for( ActorRef worker : workers ){
			
			Timeout timeout = new Timeout(Duration.create(10, "seconds"));
			Future<Object> future = Patterns.ask(worker, new SetOneToManyContext(this.graph,sampleSet), timeout);
			Boolean result = (Boolean) Await.result(future, timeout.duration());
		}
		
		this.jobSize = 0;
		this.jobsReturned=0;
		for(int i=0; i<this.jobSpec.from.featureCount(); i++){
			PointFeature from = this.jobSpec.from.getFeature(i);
			router.route(new OneToManyRequest(from, this.jobSpec.date), getSelf());
			this.jobSize += 1;
		}
	}

	private void onMsgGetGraph(Graph graph) {
		log.debug("got graph: {}", graph );
		
		this.graph = graph;
		status = Status.READY;
		getSelf().tell(new StartWorkers(), getSelf());
	}

	private void onMsgJobSliceSpec(JobSliceSpec jobSpec) {		
		// bond to the jobmanager that sent this message
		this.jobManager = getSender();
		
		log.debug( "got job slice: {}", jobSpec );
		
		this.jobSpec = jobSpec;
		
		// if the current graph isn't the graph specified by the job, kick the graph builder into action
		if(graph==null || !curGraphId.equals(jobSpec.bucket)){
			curGraphId = jobSpec.bucket;
			graphBuilder.tell(new BuildGraph(jobSpec.bucket), getSelf());
			status = Status.BUILDING_GRAPH;
		} else {
			getSelf().tell(new StartWorkers(), getSelf());
		}
		
	}

}
