package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.ArrayList;

import org.opentripplanner.routing.services.GraphService;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
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

import com.conveyal.otpac.ClusterGraphService;
import com.conveyal.otpac.PointSetDatastore;
import com.conveyal.otpac.message.AnalystClusterRequest;
import com.conveyal.otpac.message.AssignExecutive;
import com.conveyal.otpac.message.BuildGraph;
import com.conveyal.otpac.message.CancelJob;
import com.conveyal.otpac.message.DoneAssigningExecutive;
import com.conveyal.otpac.message.GetWorkerStatus;
import com.conveyal.otpac.message.ProcessClusterRequests;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.message.WorkerStatus;
import com.typesafe.config.Config;

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
	private ActorRef graphBuilder;
	private ActorRef executive;
	
	private Boolean workOffline;

	private GraphService graphService = null;
	private Status status;

	private int nWorkers;
	private PointSetDatastore s3Datastore;

	/** How many requests are outstanding? */
	private int outstandingRequests;
	
	/** Are we waiting for the queue to empty so we can swap out the graph? */
	private boolean waitingForQueueToEmptyAndGraphToBuild = false;
	
	/** What do we need to do once we get the new graph? */
	private ProcessClusterRequests stalledRequests;
	
	/** The number of requests this worker manager gets at a time */
	public int chunkSize;

	public WorkerManager(Integer nWorkers, Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		if(nWorkers == null)
			nWorkers = Runtime.getRuntime().availableProcessors() / 2;
		
		Config config = context().system().settings().config();
		String s3ConfigFilename = null;
		
		/** Chunk size starts average. If we see buffer over- or underruns we change it dynamically */
		chunkSize = 100;

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
			ActorRef worker = getContext().actorOf(Props.create(SPTWorker.class, s3Datastore), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
			workers.add(worker);
		}
		
		router = new Router(new RoundRobinRoutingLogic(), routees);

		System.out.println("worker-manager: starting " + nWorkers + " workers");
		status = Status.READY;

	}

	@Override
	public void onReceive(Object message) throws Exception {		
		if (message instanceof ProcessClusterRequests) {
			onMsgProcessClusterRequests((ProcessClusterRequests) message);
		} else if (message instanceof AssignExecutive) {
			onMsgAssignExecutive((AssignExecutive) message);
		} else if (message instanceof org.opentripplanner.standalone.Router) {
			onMsgGetRouter((org.opentripplanner.standalone.Router) message);
		} else if (message instanceof WorkResult) {
			onMsgWorkResult((WorkResult) message);
		} else if (message instanceof CancelJob ){
			onMsgCancelJob((CancelJob)message);
		} else if (message instanceof Terminated){
			onMsgTerminated((Terminated)message);
		} else if (message instanceof ActorIdentity){
			onMsgActorIdentity((ActorIdentity)message);
		} else if (message instanceof GetWorkerStatus) {
			onMsgGetWorkerStatus((GetWorkerStatus) message);
		} else {
			unhandled(message);
		}
	}

	/** Report our status back to the executive */
	private void onMsgGetWorkerStatus(GetWorkerStatus message) {
		String id = this.otpRouter != null ? this.otpRouter.id : null;
		
		// the buffer is backing up
		if (outstandingRequests > chunkSize)
			chunkSize *= 0.667;
		
		getSender().tell(new WorkerStatus(outstandingRequests, chunkSize, id,
				waitingForQueueToEmptyAndGraphToBuild), getSelf());
	}

	/** Process requests */
	private void onMsgProcessClusterRequests(ProcessClusterRequests pcr) {
		System.out.println("Process cluster requests.");
		
		if (workers.isEmpty())
			createAndRouteWorkers();
		
		if (this.otpRouter == null || !this.otpRouter.id.equals(pcr.graphId)) {
			this.waitingForQueueToEmptyAndGraphToBuild = true;
			this.stalledRequests = pcr;
			
			if (outstandingRequests == 0) {
				// no need to wait to build graph
				graphBuilder.tell(new BuildGraph(stalledRequests.graphId), getSelf());
			}
		}
		else {
			enqueueRequests(pcr);
		}
	}
	
	/** Enqueue requests for processing */
	public void enqueueRequests (ProcessClusterRequests pcr) {
		outstandingRequests += pcr.requests.length;
		
		for (AnalystClusterRequest req : pcr.requests) {
			// NOTE: the queue will never empty if req is of a type not handled by SPTWorker.
			router.route(req, getSelf());
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
				
		// TODO: should this be in actor identity?
		getContext().watch(this.executive);
		
	}

	private void onMsgWorkResult(WorkResult res) throws IOException {
		outstandingRequests -= 1;
		this.executive.tell(res, getSelf());

		if (outstandingRequests == 0) {
			// grow the queue size so we don't underrun again
			// TODO: need to shrink queue size if requests get slower
			chunkSize *= 1.5;
		}
	}

	/**
	 * A graph has been built and now we should use it.
	 */
	private void onMsgGetRouter(org.opentripplanner.standalone.Router router) throws Exception {
		log.debug("got router: {}", router);

		// if requests are not stalled, we don't want to change graphs mid-stream
		if (!waitingForQueueToEmptyAndGraphToBuild)
			return;
				
		// make sure it's the right graph
		if (!router.id.equals(stalledRequests.graphId)) {
			// build it again
			graphBuilder.tell(new BuildGraph(stalledRequests.graphId), getSelf());
		}
		else {
			this.otpRouter = router;

			// send graph to all workers
			for (ActorRef worker : workers) {
				Timeout timeout = new Timeout(Duration.create(10, "seconds"));
				Future<Object> future = Patterns.ask(worker, new SetOneToManyContext(this.otpRouter), timeout);
				Await.result(future, timeout.duration());
			}
			
			enqueueRequests(this.stalledRequests);
			this.waitingForQueueToEmptyAndGraphToBuild = false;
		}
	}
}
