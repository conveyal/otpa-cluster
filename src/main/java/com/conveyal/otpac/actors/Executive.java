package com.conveyal.otpac.actors;

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
		} else if (msg instanceof BufferFillRequest) {
			onMsgBufferFillRequest((BufferFillRequest) msg);
		} else if (msg instanceof CancelJob) {
			onMsgCancelJob((CancelJob) msg);
		} else if (msg instanceof Terminated) {
			onMsgTerminated();
		} else if(msg instanceof String){
			getSender().tell(msg, getSelf());
		} 
	}

	private void onMsgBufferFillRequest(BufferFillRequest msg) {
		sendJobsToWorkerManager(msg.routerId, getSender().path().toString(), msg.size);
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
		long complete = js.jobsSentToWorkers;
		JobStatus stat = new JobStatus(qry.jobId, total, complete);
		getSender().tell(stat, getSelf());
	}

	private void onMsgAddWorkerManager(AddWorkerManager aw) throws Exception {
		ActorRef remoteManager = aw.workerManager;
		this.wmPathActorRefs.put(remoteManager.path().toString(), remoteManager);
		
		// watch for termination
		getContext().watch(remoteManager);
		System.out.println("add worker " + remoteManager);
		
		//Timeout timeout = new Timeout(Duration.create(60, "seconds"));
		//Future<Object> future = Patterns.ask(remoteManager, new AssignExecutive(), timeout);
		//Await.result( future, timeout.duration() );
		
		// not clear why we do this twice.
		remoteManager.tell(new AssignExecutive(), getSelf());
		workerManagers.put(remoteManager.path().toString(), null);
		
		freeWorkerManagers.add(remoteManager.path().toString());
		
		getSender().tell(new Boolean(true), getSelf());
	}

	private void onMsgJobResultQuery(JobResultQuery jr) {
		ArrayList<WorkResult> res = jobResults.get(jr.jobId);
		getSender().tell(new JobResult(res), getSelf());
	}

	private void onMsgWorkResult(WorkResult wr) {
		jobResults.get(wr.jobId).add(wr);
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
		
		// TODO: record what we gave the worker manager so that we can keep track when it
		// comes back and restart if need be.
		
		return reqs.size();
	}
	
	private void onMsgJobSpec(JobSpec jobSpec) throws Exception {
		jobSpec.jobId = nextJobId++;
		
		// add to queue
		if (!multipointQueues.containsKey(jobSpec.graphId))
			multipointQueues.put(jobSpec.graphId, new ArrayList<JobSpec>(1)); 
		
		multipointQueues.get(jobSpec.graphId).add(jobSpec);
		
		// make a place to catch the results of the job
		jobResults.put(jobSpec.jobId, new ArrayList<WorkResult>());
		
		// should we send jobs now, or are there a workers working on this already?
		Set<String> activeWorkerManagers = getWorkerManagersForGraph(jobSpec.graphId);
		
		for (String workerManager : activeWorkerManagers) {
			if (freeWorkerManagers.contains(workerManager)) {
				// this manager is free, send it some bits of this job
				// we don't know how much buffer it has, so send it 32 jobs to start
				sendJobsToWorkerManager(jobSpec.graphId, workerManager, 32);
			}
		}

		if (activeWorkerManagers.size() == 0) {
			// find the free worker manager with the oldest graph, and give it this graph.
			long min = Long.MAX_VALUE;
			String best = null;
			
			for (String freeWorkerManager : freeWorkerManagers) {
				Long lastUsedTime = graphLastUsedTime.get(workerManagers.get(freeWorkerManager));
				
				if (lastUsedTime == null) {
					min = 0;
					best = freeWorkerManager;
					break;
				}
				
				if (lastUsedTime < min) {
					min = lastUsedTime;
					best = freeWorkerManager;
				}
			}
		
			// if there are no free worker managers, this job will have to wait.
			// it will be run when a worker manager becomes free.
			if (best != null) {
				sendJobsToWorkerManager(jobSpec.graphId, best, 32);
			}
		}
		
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
}
