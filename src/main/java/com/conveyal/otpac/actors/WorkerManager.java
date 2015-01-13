package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.ArrayList;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.services.GraphService;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.ClusterGraphService;
import com.conveyal.otpac.PointSetDatastore;
import com.conveyal.otpac.message.BuildGraph;
import com.conveyal.otpac.message.CancelJob;
import com.conveyal.otpac.message.DoneAssigningExecutive;
import com.conveyal.otpac.message.JobSliceDone;
import com.conveyal.otpac.message.JobSliceSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;
import com.conveyal.otpac.message.OneToManyProfileRequest;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.StartWorkers;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.message.AssignExecutive;
import com.typesafe.config.Config;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Identify;
import akka.actor.Props;
import akka.actor.Terminated;
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

	private ArrayList<ActorRef> workers;
	private Router router;
	
	/**
	 * This router contains the graph and the graph ID.
	 * It is unfortunate that there is a name collision between OTP and Akka.
	 */
	private org.opentripplanner.standalone.Router otpRouter;
	private ActorRef jobManager;
	private ActorRef graphBuilder;
	private ActorRef executive;
	
	private Boolean workOffline;

	private GraphService graphService = null;
	private Status status;

	private JobSliceSpec slice = null;
	private int nWorkers;
	private PointSetDatastore s3Datastore;

	public WorkerManager(Integer nWorkers, Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		if(nWorkers == null)
			nWorkers = Runtime.getRuntime().availableProcessors();

		Config config = context().system().settings().config();
		String s3ConfigFilename = null;

		if (config.hasPath("s3.credentials.filename"))
			s3ConfigFilename = config.getString("s3.credentials.filename");
		
		this.workOffline = workOffline;

		graphService = new ClusterGraphService(s3ConfigFilename, workOffline, graphsBucket);
		
		s3Datastore = new PointSetDatastore(10, s3ConfigFilename, workOffline, pointsetsBucket);
		
		this.nWorkers = nWorkers;
		this.workers = new ArrayList<ActorRef>();

		graphBuilder = getContext().actorOf(Props.create(GraphBuilder.class, graphService), "builder");

		System.out.println("starting worker-manager with " + nWorkers + " workers");
		status = Status.READY;
	}

	private void createAndRouteWorkers() {
		ArrayList<Routee> routees = new ArrayList<Routee>();
		
		for (int i = 0; i < this.nWorkers; i++) {
			ActorRef worker = getContext().actorOf(Props.create(SPTWorker.class), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
			workers.add(worker);
		}
		
		router = new Router(new RoundRobinRoutingLogic(), routees);

		System.out.println("worker-manager: starting " + nWorkers + " workers");
		status = Status.READY;

	}

	@Override
	public void onReceive(Object message) throws Exception {		
		if (message instanceof JobSliceSpec) {
			onMsgJobSliceSpec((JobSliceSpec) message);
		} else if (message instanceof AssignExecutive) {
			onMsgAssignExecutive((AssignExecutive) message);
		} else if (message instanceof org.opentripplanner.standalone.Router) {
			onMsgGetRouter((org.opentripplanner.standalone.Router) message);
		} else if (message instanceof StartWorkers) {
			onMsgStartWorkers();
		} else if (message instanceof WorkResult) {
			onMsgWorkResult((WorkResult) message);
		} else if (message instanceof JobStatusQuery) {
			getSender().tell(new JobStatus(getSelf(), curJobId, jobSize, jobsReturned), getSelf());
		} else if (message instanceof CancelJob ){
			onMsgCancelJob((CancelJob)message);
		} else if (message instanceof Terminated){
			onMsgTerminated((Terminated)message);
		} else if (message instanceof ActorIdentity){
			onMsgActorIdentity((ActorIdentity)message);
		} else {
			unhandled(message);
		}
	}

	private void onMsgActorIdentity(ActorIdentity actorId) {
		System.out.println("#####GOT ACTOR IDENTITY#####");

        this.executive = actorId.getRef();
	}

	private void onMsgTerminated(Terminated message) {
		//TODO check that it's the executive terminating
		
		this.executive = null;
		cancelAllWorkers();
	}

	private void onMsgCancelJob(CancelJob message) {
		cancelAllWorkers();
		
		getSender().tell(new Boolean(true), getSelf());
	}

	private void cancelAllWorkers() {
		// stop all the worker actors
		for( ActorRef worker : this.workers ){
			this.getContext().system().stop(worker);
		}
		
		// delete the actor refs from the worker list
		workers.clear();
	}

	private void onMsgAssignExecutive(AssignExecutive exec) throws Exception {
		// we set up a quickie "good enough" executive reference and then signal to the exec caller
		// that we're done. The caller then unblocks, and we can establish an official reference
		// and set up watching
		this.executive = getSender();
		this.executive.tell(new DoneAssigningExecutive(), getSelf());
		
		// set up a watch on exec
		
		// get an ActorRef for the executive via official safe channels
		this.executive.tell(new Identify("1"), getSelf());
				
		getContext().watch(this.executive);
		
	}

	private void onMsgWorkResult(WorkResult res) throws IOException {
		jobsReturned += 1;
		log.debug("got: {}", res);
		log.debug("{}/{} jobs returned", jobsReturned, jobSize);

		if (res.success) {
			jobManager.forward(res, getContext());
		}

		if (jobsReturned == jobSize) {
			jobManager.tell(new JobSliceDone(), getSelf());
		}
	}

	private void onMsgStartWorkers() throws Exception {
		log.debug("set the workers doing their thing");
		
		if(workers.isEmpty()){
			createAndRouteWorkers();
		}

		PointSet fromAll = s3Datastore.getPointset(this.slice.jobSpec.fromPtsLoc);
		PointSet fromPts;
		
		if (this.slice.fromPtsStart == null && this.slice.fromPtsEnd == null && this.slice.jobSpec.subsetIds != null)
			fromPts = fromAll.slice(this.slice.jobSpec.subsetIds);
		
		else if (this.slice.fromPtsStart != null && this.slice.fromPtsEnd != null && this.slice.jobSpec.subsetIds != null)
			// we slice by indices first, because the indices will change when we slice by IDs. Some of the IDs will
			// no longer be in the point set, which is fine as pointset.slice(List<String>) simply ignores them.
			// (confirmed)
			fromPts = fromAll.slice(this.slice.fromPtsStart, this.slice.fromPtsEnd).slice(this.slice.jobSpec.subsetIds);
		
		else if (this.slice.fromPtsStart != null && this.slice.fromPtsEnd != null && this.slice.jobSpec.subsetIds == null)
			fromPts = fromAll.slice(this.slice.fromPtsStart, this.slice.fromPtsEnd);
		
		else
			fromPts = fromAll;
		
		PointSet toPts = s3Datastore.getPointset(this.slice.jobSpec.toPtsLoc);

		SampleSet sampleSet = toPts.getSampleSet(otpRouter.graph);

		// send graph to all workers
		for (ActorRef worker : workers) {

			Timeout timeout = new Timeout(Duration.create(10, "seconds"));
			Future<Object> future = Patterns.ask(worker, new SetOneToManyContext(this.otpRouter, sampleSet), timeout);
			Await.result(future, timeout.duration());
		}

		this.jobSize = 0;
		this.jobsReturned = 0;
		for (int i = 0; i < fromPts.featureCount(); i++) {
			PointFeature from = fromPts.getFeature(i);
			
			if (this.slice.jobSpec.profileRouting)
				router.route(new OneToManyProfileRequest(from, this.slice.jobSpec.profileOptions), getSelf());
			else
				router.route(new OneToManyRequest(from, this.slice.jobSpec.options), getSelf());
			
			this.jobSize += 1;
		}
	}

	private void onMsgGetRouter(org.opentripplanner.standalone.Router router) {
		log.debug("got router: {}", router);

		this.otpRouter = router;
		status = Status.READY;
		getSelf().tell(new StartWorkers(), getSelf());
	}

	private void onMsgJobSliceSpec(JobSliceSpec jobSpec) {
		// bond to the jobmanager that sent this message
		this.jobManager = getSender();

		log.debug("got job slice: {}", jobSpec);

		this.slice = jobSpec;
		this.curJobId = jobSpec.jobSpec.jobId;

		// if the current graph isn't the graph specified by the job, kick the
		// graph builder into action
		if (otpRouter == null || !otpRouter.id.equals(jobSpec.graphId)) {
			otpRouter = null;
			graphBuilder.tell(new BuildGraph(jobSpec.graphId), getSelf());
			status = Status.BUILDING_GRAPH;
		} else {
			getSelf().tell(new StartWorkers(), getSelf());
		}

	}

}
