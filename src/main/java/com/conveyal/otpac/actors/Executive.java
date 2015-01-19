package com.conveyal.otpac.actors;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;

import com.conveyal.otpac.PointSetDatastore;
import com.conveyal.otpac.message.*;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class Executive extends UntypedActor {

	private Map<Integer, ArrayList<WorkResult>> jobResults;
	private Map<String,ActorRef> wmPathActorRefs; // path->canonical actorref
	private Map<String, String> workerManagers; // path -> current router ID 
	private Map<String, Long> graphLastUsedTime; // the last time a graph was used for a single point request
	private Set<String> freeWorkerManagers; // worker managers not currently being used
	
	private static int nextJobId = 0;
	
	/**
	 * Multipoint query queues (per graph)
	 */
	private Map<String, List<JobSpec>> multipointQueues;
	
	/**
	 * Multipoint queue sizes, per graph.
	 * We can't just use the length of multipointQueues, because that is the number of job specs,
	 * which are made up of thousands of jobs and may vary widely in size.
	 */
	private TObjectLongMap<String> multipointQueueSize = new TObjectLongHashMap<String>(4);
	
	/**
	 * Index of job specs by job ID.
	 */
	private Map<Integer, JobSpec> jobSpecsByJobId;
	
	/**
	 * Pointsets come from here.
	 */
	private PointSetDatastore pointsetDatastore;
	
	String pointsetsBucket, graphsBucket, s3CredentialsFilename;
	
	Boolean workOffline;
	
	/**
	 * A queue for single-point jobs.
	 */
	//private List<SinglePointJobSpec> singlePointQueue;

	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public Executive(Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		wmPathActorRefs = new HashMap<String,ActorRef>();
		workerManagers = new HashMap<String, String>();
		freeWorkerManagers = new HashSet<String>();
		graphLastUsedTime = new HashMap<String, Long>();
		jobSpecsByJobId = new HashMap<Integer, JobSpec>();
				
		this.graphsBucket = graphsBucket;
		this.pointsetsBucket = pointsetsBucket;
		
		Config config = context().system().settings().config();
		String s3ConfigFilename = null;

		if (config.hasPath("s3.credentials.filename"))
			s3ConfigFilename = config.getString("s3.credentials.filename");
		
		this.pointsetDatastore = new PointSetDatastore(10, s3ConfigFilename, workOffline, pointsetsBucket);

		this.workOffline = workOffline;
		
		this.multipointQueues = new ConcurrentHashMap<String, List<JobSpec>>(4);
		
		// set up polling on the workers
		ActorSystem system = getContext().system();
		system.scheduler().schedule(Duration.create(10, "seconds"), Duration.create(5, "seconds"),
				getSelf(), new Poll(), system.dispatcher(), null);
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof JobSpec) {
			onMsgJobSpec((JobSpec) msg);
		} else if (msg instanceof WorkResult) {
			onMsgWorkResult((WorkResult) msg);
		} else if (msg instanceof JobResultQuery) {
			onMsgJobResultQuery((JobResultQuery) msg);
		} else if (msg instanceof AddWorkerManager) {
			onMsgAddWorkerManager((AddWorkerManager) msg);
		} else if (msg instanceof JobStatusQuery) {
			onMsgJobStatusQuery((JobStatusQuery) msg);
		} else if (msg instanceof CancelJob) {
			onMsgCancelJob((CancelJob) msg);
		} else if (msg instanceof Terminated) {
			onMsgTerminated();
		} else if (msg instanceof Poll) {
			onMsgPoll();
		// TODO: dead code?
		} else if(msg instanceof String){
			getSender().tell(msg, getSelf());
		} else if (msg instanceof WorkerStatus) {
			onMsgWorkerStatus((WorkerStatus) msg);
		}
	}

	private void onMsgWorkerStatus(WorkerStatus msg) {
		String workerManager = getSender().path().toString();
		
		workerManagers.put(workerManager, msg.graph);
		
		// If the queue is draining, send some more requests
		if (msg.queueSize < msg.chunkSize && !msg.buildingGraph) {
			// decide what to do: more from the same graph?
			if (msg.graph != null && multipointQueues.get(msg.graph).size() > 0) {
				sendJobsToWorkerManager(msg.graph, workerManager, msg.chunkSize);
			}
			else {
				// TODO: don't evict graphs that are needed for single point mode
				// first: any non-empty queues without a worker
				// next: largest queue/worker ratio
				// we cannot divide by zero; if any queue had no workers it would have gotten caught by the previous
				// loop.
				String chosenGraph = null;
				
				// jobs per worker
				long worstRatio = 0;
				
				for (String graph : multipointQueueSize.keys(new String[multipointQueueSize.size()])) {
					long queueSize = multipointQueueSize.get(graph);
					
					if (queueSize == 0)
						// nothing queued, no need to assign any workers to this graph
						continue;
					
					int nWorkers = getWorkerManagersForGraph(graph).size();
					
					// no workers on a given job
					if (nWorkers == 0) {
						chosenGraph = graph;
						break;
					}
					
					long ratio = queueSize / nWorkers;
					
					if (ratio > worstRatio) {
						worstRatio = queueSize;
						chosenGraph = graph;
					}
				}
				
				if (chosenGraph != null)
					sendJobsToWorkerManager(chosenGraph, workerManager, msg.chunkSize);
			}
		}
	}

	private void onMsgCancelJob(CancelJob msg) {
		// remove from queue. don't worry about the small number of requests
		// in workermanagers queues; they will complete before we could reasonably do
		// anything about it anyhow. We'll ignore the work results when they come back.
		// TODO: implement
	}

	private void onMsgTerminated() {
		// TODO: implement
		/*ActorRef dead = getSender();
		
		// if the workermanager is assigned to a jobmanager, tell the jobmanager to remove it
		Integer jobId = getWorkerManagerJob( dead );
		if(jobId != null){
			getJobManager(jobId).tell( new RemoveWorkerManager(dead), getSelf() );
			freeWorkerManager(dead.path().toString()); //TODO double check this
		}
		
		// delete the workermanager from the roster
		deleteWorkerManager(dead);*/
	}

	private void deleteWorkerManager(ActorRef dead) {
		this.workerManagers.remove(dead.path().toString());
	}

	private void onMsgJobStatusQuery(JobStatusQuery qry) throws Exception {
		JobSpec js = jobSpecsByJobId.get(qry.jobId);
		// this is not inefficient because the pointset is cached
		long total = js.getOrigins(pointsetDatastore).capacity;
		long complete = jobResults.get(js.jobId).size();
		JobStatus stat = new JobStatus(qry.jobId, total, complete);
		getSender().tell(stat, getSelf());
	}

	private void onMsgAddWorkerManager(AddWorkerManager aw) throws Exception {
		ActorRef remoteManager = aw.workerManager;
		this.wmPathActorRefs.put(remoteManager.path().toString(), remoteManager);
		
		// watch for termination
		getContext().watch(remoteManager);
		System.out.println("add worker " + remoteManager);
		
		remoteManager.tell(new AssignExecutive(), getSelf());
		workerManagers.put(remoteManager.path().toString(), null);
		
		freeWorkerManagers.add(remoteManager.path().toString());
		
		getSender().tell(new Boolean(true), getSelf());
		
		// it will get shuffled into the rotation on the next poll, do nothing more.
	}

	private void onMsgJobResultQuery(JobResultQuery jr) {
		ArrayList<WorkResult> res = jobResults.get(jr.jobId);
		getSender().tell(new JobResult(res), getSelf());
	}

	private void onMsgWorkResult(WorkResult wr) {
		jobResults.get(wr.jobId).add(wr);
		JobSpec js = jobSpecsByJobId.get(wr.jobId);
		if (js.callback != null) {
			js.callback.tell(wr, getSelf());
		}
	}

	/**
	 * Send up to count jobs for the specified graph to the specified worker manager.
	 * Return the number of jobs sent.
	 */
	private int sendJobsToWorkerManager (String graphId, String workerManager, int count) {
		if (count == 0)
			return 0;
		
		// pull some jobs off the queue
		List<JobSpec> queue = multipointQueues.get(graphId);
		
		List<AnalystClusterRequest> reqs = new ArrayList<AnalystClusterRequest>(count);
		
		while (reqs.size() < count && queue.size() > 0) {
			// pull one job off the queue
			JobSpec js = queue.get(0);
			PointSet origins = js.getOrigins(this.pointsetDatastore);
			
			while (js.jobsSentToWorkers < origins.capacity && reqs.size() < count) {
				PointFeature origin = origins.getFeature(js.jobsSentToWorkers);

				if (js.profileRouting) {
					reqs.add(new OneToManyProfileRequest(origin, js.toPtsLoc, js.profileOptions, js.graphId));
				}
				else {
					reqs.add(new OneToManyRequest(origin, js.toPtsLoc, js.options, js.graphId));
				}
				
				js.jobsSentToWorkers++;
			}
		}
		
		// mark the workermanager as busy or not.
		if (reqs.size() == 0) {
			freeWorkerManagers.add(workerManager);
		}
		else {
			freeWorkerManagers.remove(workerManager);
		}
		
		ProcessClusterRequests pcr =
				new ProcessClusterRequests(graphId, reqs.toArray(new AnalystClusterRequest[reqs.size()]));
		
		wmPathActorRefs.get(workerManager).tell(pcr, getSelf());
		
		// make the queue smaller
		multipointQueueSize.put(graphId, multipointQueueSize.get(graphId) - reqs.size());
		
		// TODO: record what we gave the worker manager so that we can keep track when it
		// comes back and restart if need be.
		
		return reqs.size();
	}
	
	private void onMsgJobSpec(JobSpec jobSpec) throws Exception {
		jobSpec.jobId = nextJobId++;
		
		// add to queue
		if (!multipointQueues.containsKey(jobSpec.graphId)) {
			multipointQueues.put(jobSpec.graphId, new ArrayList<JobSpec>(1));
			multipointQueueSize.put(jobSpec.graphId, 0);
		}
		
		// we need the origins to know job size, and this will fetch the point set or grab it from RAM.
		// it's fine to do this repeatedly, because the point set cache should be large enough that
		// subsequent calls are very fast (in-memory reference passing fast).
		PointSet origins = jobSpec.getOrigins(this.pointsetDatastore);
		
		multipointQueues.get(jobSpec.graphId).add(jobSpec);
		multipointQueueSize.put(jobSpec.graphId, multipointQueueSize.get(jobSpec.graphId) + origins.capacity);		
		
		// make a place to catch the results of the job
		jobResults.put(jobSpec.jobId, new ArrayList<WorkResult>());
		
		// save the job spec
		jobSpecsByJobId.put(jobSpec.jobId, jobSpec);
		
		getSender().tell(new JobId(jobSpec.jobId), getSelf());
	}
	
	/**
	 * Return the worker managers that are currently working on this graph.
	 */
	private Set<String> getWorkerManagersForGraph(String graphId) {
		Set<String> ret = new HashSet<String>();
		
		for (Entry<String, String> entry : workerManagers.entrySet()) {
			if (graphId.equals(entry.getValue())) {
				ret.add(entry.getKey());
			}
		}
		
		return ret;
	}
	
	/**
	 * Poll the worker managers to make sure that there aren't any free worker managers we don't
	 * know about.
	 * 
	 * This doesn't handle terminations, as the failure detector will generate Terminated messages
	 * for terminated actors. This is to ensure that there are no worker managers with empty
	 * queues.
	 */
	private void onMsgPoll () {
		for (ActorRef wm : wmPathActorRefs.values()) {
			wm.tell(new GetWorkerStatus(), getSelf());
		}
	}
	
	/**
	 * This message is sent to the executive by the scheduler every minute
	 * to poll the workers for their statuses in case any messages have gotten lost.
	 * @author matthewc
	 *
	 */
	private static class Poll { /* nothing */ }
}
