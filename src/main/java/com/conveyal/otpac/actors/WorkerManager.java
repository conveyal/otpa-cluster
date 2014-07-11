package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.ArrayList;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.graph.Graph;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.S3Datastore;
import com.conveyal.otpac.message.BuildGraph;
import com.conveyal.otpac.message.JobSliceDone;
import com.conveyal.otpac.message.JobSliceSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.StartWorkers;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.message.AssignExecutive;

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

public class WorkerManager extends UntypedActor {
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	public enum Status {
		READY, BUILDING_GRAPH, WORKING
	};

	private int curJobId = -1;
	private long jobSize = -1;
	private long jobsReturned = 0;
	private ArrayList<WorkResult> jobResults;
	private ArrayList<ActorRef> workers;
	private Router router;
	private ActorRef jobManager;
	private ActorRef graphBuilder;
	private ActorRef executive;

	private String curGraphId = null;
	private Graph graph = null;
	private Status status;

	private JobSliceSpec jobSpec = null;
	private S3Datastore s3Datastore;

	WorkerManager() {
		this(Runtime.getRuntime().availableProcessors());
	}

	WorkerManager(int nWorkers) {
		String s3ConfigFilename = context().system().settings().config().getString("s3.credentials.filename");
		s3Datastore = new S3Datastore(s3ConfigFilename);

		ArrayList<Routee> routees = new ArrayList<Routee>();
		workers = new ArrayList<ActorRef>();
		for (int i = 0; i < nWorkers; i++) {
			ActorRef worker = getContext().actorOf(Props.create(SPTWorker.class), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
			workers.add(worker);
		}
		router = new Router(new RoundRobinRoutingLogic(), routees);

		graphBuilder = getContext().actorOf(Props.create(GraphBuilder.class), "builder");

		System.out.println("starting manager with " + nWorkers + " workers");
		status = Status.READY;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		log.info("got message {}", message);
		
		if (message instanceof JobSliceSpec) {
			onMsgJobSliceSpec((JobSliceSpec) message);
		} else if (message instanceof AssignExecutive) {
			onMsgAssignExecutive((AssignExecutive) message);
		} else if (message instanceof Graph) {
			onMsgGetGraph((Graph) message);
		} else if (message instanceof StartWorkers) {
			onMsgStartWorkers();
		} else if (message instanceof WorkResult) {
			onMsgWorkResult((WorkResult) message);
		} else if (message instanceof JobStatusQuery) {
			getSender().tell(new JobStatus(getSelf(), curJobId, jobsReturned / (float) jobSize), getSelf());
		} else {
			unhandled(message);
		}
	}

	private void onMsgAssignExecutive(AssignExecutive exec) {
		this.executive = getSender();
		log.debug("assigned to executive: {}", this.executive);
		this.executive.tell(new Boolean(true), getSelf());
	}

	private void onMsgWorkResult(WorkResult res) throws IOException {
		jobsReturned += 1;
		log.debug("got: {}", res);
		log.debug("{}/{} jobs returned", jobsReturned, jobSize);

		if (res.success) {
			jobManager.forward(res, getContext());
			;
		}

		if (jobsReturned == jobSize) {
			jobManager.tell(new JobSliceDone(), getSelf());
		}
	}

	private void onMsgStartWorkers() throws Exception {
		log.debug("set the workers doing their thing");

		PointSet fromAll = s3Datastore.getPointset(this.jobSpec.fromPtsLoc);
		PointSet fromPts = fromAll.slice(this.jobSpec.fromPtsStart, this.jobSpec.fromPtsEnd);

		PointSet toPts = s3Datastore.getPointset(this.jobSpec.toPtsLoc);

		SampleSet sampleSet = new SampleSet(toPts, this.graph.getSampleFactory());

		// send graph to all workers
		for (ActorRef worker : workers) {

			Timeout timeout = new Timeout(Duration.create(10, "seconds"));
			Future<Object> future = Patterns.ask(worker, new SetOneToManyContext(this.graph, sampleSet), timeout);
			Await.result(future, timeout.duration());
		}

		this.jobSize = 0;
		this.jobsReturned = 0;
		for (int i = 0; i < fromPts.featureCount(); i++) {
			PointFeature from = fromPts.getFeature(i);
			router.route(new OneToManyRequest(from, this.jobSpec.date), getSelf());
			this.jobSize += 1;
		}
	}

	private void onMsgGetGraph(Graph graph) {
		log.debug("got graph: {}", graph);

		this.graph = graph;
		status = Status.READY;
		getSelf().tell(new StartWorkers(), getSelf());
	}

	private void onMsgJobSliceSpec(JobSliceSpec jobSpec) {
		// bond to the jobmanager that sent this message
		this.jobManager = getSender();

		log.debug("got job slice: {}", jobSpec);

		this.jobSpec = jobSpec;

		// if the current graph isn't the graph specified by the job, kick the
		// graph builder into action
		if (graph == null || !curGraphId.equals(jobSpec.bucket)) {
			curGraphId = jobSpec.bucket;
			graphBuilder.tell(new BuildGraph(jobSpec.bucket), getSelf());
			status = Status.BUILDING_GRAPH;
		} else {
			getSelf().tell(new StartWorkers(), getSelf());
		}

	}

}
