package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.ArrayList;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import akka.pattern.Patterns;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.util.Timeout;

import com.conveyal.otpac.message.AddWorkerManager;
import com.conveyal.otpac.message.AnalystClusterRequest;
import com.conveyal.otpac.message.GetGraph;
import com.conveyal.otpac.message.GetWorkerStatus;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.message.WorkerStatus;
import com.typesafe.config.Config;

// note: many fields are protected rather than private, because they are overridden or used in
// test subclasses to make workermanagers that misbehave.
public class WorkerManager extends UntypedActor {
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	public static enum Status {
		READY, BUILDING_GRAPH, WORKING
	};

	protected ArrayList<ActorRef> workers;
	protected Router router;
	
	/**
	 * This router contains the graph and the graph ID.
	 * It is unfortunate that there is a name collision between OTP and Akka.
	 */
	protected org.opentripplanner.standalone.Router otpRouter;
	protected ActorRef graphBuilder;
	private ActorRef executive;
	
	private Boolean workOffline;

	protected Status status;

	private int nWorkers;

	/** How many requests are outstanding? */
	private int outstandingRequests;
	
	/** Are we waiting for the queue to empty so we can swap out the graph? */
	private boolean waitingForQueueToEmptyAndGraphToBuild = false;
	
	/** The graph to build once the queue empties */
	private String graphToBuild;
	
	/** The number of requests this worker manager gets at a time */
	public int chunkSize;
	
	/** How many requests have we received since the last poll? */
	private int requestsReceivedSinceLastPoll = 0;

	/**
	 * The root supervision strategy is to always resume, since we don't want to lose state contained
	 * in Executive and WorkerManager. But if graph builders or SPTWorkers die, we want to restart them
	 * to get rid of any possible tainted state. 
	 */
	private static final SupervisorStrategy supervisorStrategy =
			new OneForOneStrategy(10, Duration.create(1, "minute"),
					new Function<Throwable, Directive> () {

						@Override
						public Directive apply(Throwable arg0) throws Exception {
							return SupervisorStrategy.restart();
						}
				
			});
	
	public WorkerManager(ActorRef executive, Integer nWorkers, Boolean workOffline,
			String graphsBucket, String pointsetsBucket) {		
		if(nWorkers == null)
			nWorkers = Runtime.getRuntime().availableProcessors();

		Config config = context().system().settings().config();
		String s3ConfigFilename = null;
		
		this.executive = executive;
		
		// Chunk size starts average. If we see buffer over- or underruns we change it dynamically
		chunkSize = 100;

		if (config.hasPath("s3.credentials.filename"))
			s3ConfigFilename = config.getString("s3.credentials.filename");
		
		this.workOffline = workOffline;
				
		this.nWorkers = nWorkers;
		this.workers = new ArrayList<ActorRef>();

		graphBuilder = getContext().actorOf(Props.create(GraphBuilder.class, s3ConfigFilename, workOffline, graphsBucket, pointsetsBucket), "builder");

		System.out.println("starting worker-manager with " + nWorkers + " workers");
		
		createAndRouteWorkers();
		
		status = Status.READY;
		
		// connect to our executive, to get some jobs
		getSelf().tell(new ConnectToExecutive(), getSelf());
	}

	protected void createAndRouteWorkers() {
		ArrayList<Routee> routees = new ArrayList<Routee>();
		
		for (int i = 0; i < this.nWorkers; i++) {
			ActorRef worker = getContext().actorOf(Props.create(SPTWorker.class, graphBuilder), "worker-" + i);
			routees.add(new ActorRefRoutee(worker));
			workers.add(worker);
		}
		
		router = new Router(new RoundRobinRoutingLogic(), routees);

		System.out.println("worker-manager: starting " + nWorkers + " workers");
		status = Status.READY;

	}

	@Override
	public void onReceive(Object message) throws Exception {		
		if (message instanceof org.opentripplanner.standalone.Router) {
			onMsgGetRouter((org.opentripplanner.standalone.Router) message);
		} else if (message instanceof ConnectToExecutive) {
			onMsgConnectToExecutive();
		} else if (message instanceof WorkResult) {
			onMsgWorkResult((WorkResult) message);
		} else if (message instanceof GetGraph) {
			onMsgGetGraph((GetGraph) message);
		} else if (message instanceof AnalystClusterRequest) {
			onMsgAnalystClusterRequest((AnalystClusterRequest) message);
		} else if (message instanceof Terminated){
			onMsgTerminated((Terminated)message);
		} else if (message instanceof GetWorkerStatus) {
			onMsgGetWorkerStatus((GetWorkerStatus) message);
		} else {
			unhandled(message);
		}
	}
	
	/**
	 * Connect to the executive, in a blocking way. This is
	 * done on receipt of a message from self, so that if it crashes, this actor is restarted again,
	 * rather than the entire actorsystem coming crashing to the ground.
	 * @throws Exception
	 */
	public void onMsgConnectToExecutive () throws Exception {
		// tell the executive to be ready for us
		// note that if we are restarting, this will just replace the actorref that was in the executive before
		// this is a blocking operation only to make sure it completes.
		// it's possible that we would already have requests coming in by the time this
		// happens if the actor was restarted due to a crash, but that's fine because
		// we'll just replace the actorref in the executive with another actorref pointing
		// at the same place.
		Timeout timeout = new Timeout(Duration.create(30, "seconds"));
		Future<Object> res = Patterns.ask(this.executive, new AddWorkerManager(getSelf()), timeout);
		Await.result(res, timeout.duration());
	}

	/** Report our status back to the executive */
	private void onMsgGetWorkerStatus(GetWorkerStatus message) {
		String id = this.otpRouter != null ? this.otpRouter.id : null;
		
		// the buffer is backing up
		if (outstandingRequests > chunkSize)
			chunkSize *= 0.667;
		
		// buffer underrun, ask for more this time
		else if (outstandingRequests == 0 && requestsReceivedSinceLastPoll >= chunkSize && !waitingForQueueToEmptyAndGraphToBuild)
			// do this here not in onWorkResult so that the queue cannot grow unbounded
			// suppose that the requests are being processed as fast as they are coming in
			// the queue expands 1.5x each time.
			chunkSize *= 1.5;
		
		requestsReceivedSinceLastPoll = 0;
		
		// don't let the chunk size get too small
		if (chunkSize < 10)
			chunkSize = 10;
		
		if (chunkSize > 1000)
			chunkSize = 1000;
		
		System.out.println("get status: " + outstandingRequests + " requests outstanding, " + chunkSize + " chunk size" +
				(waitingForQueueToEmptyAndGraphToBuild ? ", building graph" : ""));
		
		getSender().tell(new WorkerStatus(outstandingRequests, chunkSize, id,
				waitingForQueueToEmptyAndGraphToBuild), getSelf());
	}

	/** Build a graph in preparation for processing requests */
	private void onMsgGetGraph(GetGraph msg) {
		log.info("building graph: " + msg.graphId);
		
		if (this.otpRouter != null && msg.graphId.equals(this.otpRouter.id))
			log.warning("Got request to buid graph already contained in memory.");
		
		this.graphToBuild = msg.graphId;
		this.waitingForQueueToEmptyAndGraphToBuild = true;
		
		if (outstandingRequests == 0) {
			// no need to wait to build graph
			graphBuilder.tell(new GetGraph(graphToBuild), getSelf());
		}
	}
	
	/**
	 * Set the SPT workers processing this cluster request.
	 * @throws Exception 
	 */
	private void onMsgAnalystClusterRequest(AnalystClusterRequest req) throws Exception {
		// it's fine to drop requests at this point; they will simply be retried by the executive later
		if (waitingForQueueToEmptyAndGraphToBuild) {
			log.error("Got cluster request during graph build; ignoring");
			return;
		}
		
		if (this.otpRouter.id == null || !this.otpRouter.id.equals(req.graphId)) {
			// this can happen when the spt worker has been restarted
			log.error("Graph ID does not match, ignoring");
			return;
		}
		
		this.requestsReceivedSinceLastPoll++;
		
		outstandingRequests++;
		router.route(req, getSelf());
	}

	private void onMsgTerminated(Terminated message) {
		// TODO: attempt to reconnect		
	}

	private void onMsgWorkResult(WorkResult res) throws IOException {
		outstandingRequests--;

		this.executive.tell(res, getSelf());

		if (outstandingRequests == 0 && waitingForQueueToEmptyAndGraphToBuild) {
			// build the graph
			graphBuilder.tell(new GetGraph(graphToBuild), getSelf());
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
		if (!router.id.equals(graphToBuild)) {
			// build it again
			graphBuilder.tell(new GetGraph(graphToBuild), getSelf());
		}
		else {
			this.otpRouter = router;
			this.waitingForQueueToEmptyAndGraphToBuild = false;
			
			// the workers will request the graph as they need it
		}
	}
	
	/** assign the supervisor strategy */
	@Override
	public SupervisorStrategy supervisorStrategy () {
		return supervisorStrategy;
	}

	/** Message class to connect to the executive */
	private static class ConnectToExecutive { /* empty */ }
}
