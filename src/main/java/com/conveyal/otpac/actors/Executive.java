package com.conveyal.otpac.actors;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
import akka.actor.Cancellable;
import akka.actor.Identify;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class Executive extends UntypedActor {
	private Map<String,ActorRef> wmPathActorRefs; // path->canonical actorref
	private Map<String, String> workerManagers; // path -> current router ID 
	
	/**
	 * These are multipoint job components that were sent to workers and should have come back but
	 * didn't (because the message got dropped or the worker died), and thus should be sent again.
	 */
	private Map<String, Set<MultipointJobComponent>> overdueResponses = new HashMap<String, Set<MultipointJobComponent>>();
	
	/** Jobs that have been sent to a worker */
	private AbstractQueue<MultipointJobComponent> sent = new ConcurrentLinkedQueue<MultipointJobComponent>();
	
	/**
	 * Jobs that have been sent to a worker and have not returned. This is effectively a view of the
	 * above in HashSet format (although it does actually have distinct data backing it up).
	 * We do this so that we can remove things/compare things very quickly, but we still have
	 * the FIFO qualities of a queue.  
	 */
	private Set<MultipointJobComponent> backlog = new HashSet<MultipointJobComponent>();
	
	// the first job ID is 1 not 0, because there was once an unfortunate bug where job IDs weren't properly
	// passed but because Java initialized them to zero everything worked---until there were multiple jobs.
	private static int nextJobId = 1;
	
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
	 * The number of requests on workers for each job ID.
	 * Used to calculate job status.
	 */
	private TIntIntMap backlogByJobId = new TIntIntHashMap();
	
	/**
	 * The number of results we've gotten back for each job ID.
	 * Used to calculate job status.
	 */
	private TIntIntMap completePointsByJobId = new TIntIntHashMap();
	
	
	
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

	/**
	 * Worker polling
	 */
	private final Cancellable poll = getContext().system().scheduler().schedule(
			Duration.create(5,  "seconds"), 
			Duration.create(3, "seconds"),
			getSelf(), new Poll(), getContext().dispatcher(), null);
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	public Executive(Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		wmPathActorRefs = new HashMap<String,ActorRef>();
		workerManagers = new HashMap<String, String>();
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
		System.out.println("got worker status: " + msg.toString());
		
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
				
				if (chosenGraph != null) {
					// tell the worker to switch graphs
					// we will send it some requests once it is done and we poll it again
					getSender().tell(new BuildGraph(chosenGraph), getSelf());
				}	
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
		String dead = getSender().path().toString();
		
		log.info("disconnected from " + dead);
	}

	private void onMsgJobStatusQuery(JobStatusQuery qry) throws Exception {
		JobSpec js = jobSpecsByJobId.get(qry.jobId);
		// this is not inefficient because the pointset is cached
		long total = js.getOrigins(pointsetDatastore).capacity;
		long complete = completePointsByJobId.get(js.jobId);
		JobStatus stat = new JobStatus(qry.jobId, total, complete);
		getSender().tell(stat, getSelf());
	}

	private void onMsgAddWorkerManager(AddWorkerManager aw) throws Exception {
		ActorRef remoteManager = aw.workerManager;
		this.wmPathActorRefs.put(remoteManager.path().toString(), remoteManager);
		
		// watch for termination
		getContext().watch(remoteManager);
		
		// generate an ack back to the worker so it unblocks
		// note that we use getSender not the direct reference, because otherwise
		// Patterns.ask never completes; evidently there is a request ID burned into the sender
		// or something.
		getSender().tell(Boolean.TRUE, getSelf());		
		
		// this manager will get shuffled into the rotation on the next poll, do nothing more.
	}

	private void onMsgWorkResult(WorkResult wr) {
		JobSpec js = jobSpecsByJobId.get(wr.jobId);
		
		// remove it from the backlog
		// it will be removed from the queue by onMsgPoll, in due course
		// time does not matter as it is not used in equality (on purpose)
		MultipointJobComponent c = new MultipointJobComponent(wr.jobId, wr.point, 0);
		
		// we decrement and call the callback only if we removed it from the backlog.
		// it is possible to get the same result twice if a job was restarted and then the original job returned.
		// this way the caller can rely on only receiving a message once.
		if (backlog.remove(c)) {
			// keep track of how many have returned
			completePointsByJobId.increment(wr.jobId);
			
			backlogByJobId.adjustValue(wr.jobId, -1);
			
			if (backlogByJobId.get(wr.jobId) < 0)
				log.error("received work result and decremented backlog below 0");
			
		
			if (js.callback != null) {
				js.callback.tell(wr, getSelf());
			}
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
		
		while (reqs.size() < count && multipointQueueSize.get(graphId) > 0) {
			// if there are any jobs that need to be re-run, re-run them
			if (overdueResponses.get(graphId).size() > 0) {
				Iterator<MultipointJobComponent> it = overdueResponses.get(graphId).iterator();
				while (it.hasNext() && reqs.size() < count) {
					MultipointJobComponent c = it.next();
					// we will enqueue it again shortly
					it.remove();
					
					JobSpec js = jobSpecsByJobId.get(c.jobId);
					if (js.profileRouting) {
						reqs.add(new OneToManyProfileRequest(c.from, js.toPtsLoc, js.profileOptions, js.graphId, js.jobId));
					}
					else {
						reqs.add(new OneToManyRequest(c.from, js.toPtsLoc, js.options, js.graphId, js.jobId));
					}
					
					// make the queue smaller
					multipointQueueSize.adjustValue(graphId, -1);
				}
				
				// start again so we don't try to grab something off the multipoint queue if there
				// is nothing to be had.
				continue;
			}
			
			// pull one job off the queue
			JobSpec js = queue.get(0);
			PointSet origins = js.getOrigins(this.pointsetDatastore);
			
			while (js.jobsSentToWorkers < origins.capacity && reqs.size() < count) {
				PointFeature origin = origins.getFeature(js.jobsSentToWorkers);

				if (js.profileRouting) {
					reqs.add(new OneToManyProfileRequest(origin, js.toPtsLoc, js.profileOptions, js.graphId, js.jobId));
				}
				else {
					reqs.add(new OneToManyRequest(origin, js.toPtsLoc, js.options, js.graphId, js.jobId));
				}
				
				js.jobsSentToWorkers++;
				multipointQueueSize.adjustValue(graphId, -1);
			}
			
			if (js.jobsSentToWorkers == origins.capacity) {
				// this job is done
				queue.remove(0);
			}
		}
		
		ActorRef wmar = wmPathActorRefs.get(workerManager);
		
		// when we send a block of requests to a worker, we expect them all to return
		// within 30 seconds. If they don't, we re-send them.
		
		long sendTime = System.currentTimeMillis(); 
		for (AnalystClusterRequest req : reqs) {
			wmar.tell(req, getSelf());
			MultipointJobComponent c = new MultipointJobComponent(req.jobId, req.from, sendTime);
			sent.add(c);
			if (!backlog.add(c))
				log.error("backlog already contained " + c);
			backlogByJobId.increment(req.jobId);
		}
		
		return reqs.size();
	}
	
	private void onMsgJobSpec(JobSpec jobSpec) throws Exception {
		jobSpec.jobId = nextJobId++;
		
		// add to queue
		if (!multipointQueues.containsKey(jobSpec.graphId)) {
			multipointQueues.put(jobSpec.graphId, new ArrayList<JobSpec>(1));
			multipointQueueSize.put(jobSpec.graphId, 0);
			overdueResponses.put(jobSpec.graphId, new HashSet<MultipointJobComponent>());
			backlogByJobId.put(jobSpec.jobId, 0);
			completePointsByJobId.put(jobSpec.jobId, 0);
		}
		
		// we need the origins to know job size, and this will fetch the point set or grab it from RAM.
		// it's fine to do this repeatedly, because the point set cache should be large enough that
		// subsequent calls are very fast (in-memory reference passing fast).
		PointSet origins = jobSpec.getOrigins(this.pointsetDatastore);
		
		multipointQueues.get(jobSpec.graphId).add(jobSpec);
		multipointQueueSize.put(jobSpec.graphId, multipointQueueSize.get(jobSpec.graphId) + origins.capacity);		
		
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
	 * Poll the worker managers so we can give them work.
	 */
	private void onMsgPoll () {
		System.out.println("Polling " + wmPathActorRefs.size() + " workers");
		
		// while we're at it, mark any overdue requests for reprocessing
		long now = System.currentTimeMillis();
		// pull out all the items that are taking more than 30 seconds to process.
		while (sent.size() > 0 && now - sent.peek().sentTime > 30 * 1000) {
			MultipointJobComponent next = sent.poll();
			
			if (!backlog.contains(next))
				// yay! job has come back
				continue;
			
			// otherwise, enqueue it for reprocessing
			JobSpec js = jobSpecsByJobId.get(next.jobId);
			this.overdueResponses.get(js.graphId).add(next);
			// bump up the queue size
			this.multipointQueueSize.increment(js.graphId);
			log.warning("Reprocessing request because it did not return after 30 seconds " + next);
			
			// it's no longer backlogged, now it's queued
			this.backlogByJobId.adjustValue(js.jobId, -1);
			this.backlog.remove(next);
			
			if (this.backlogByJobId.get(js.jobId) < 0)
				log.error("decrementing backlog below 0 on reprocessing");
		}
		
		for (ActorRef wm : wmPathActorRefs.values()) {
			wm.tell(new GetWorkerStatus(), getSelf());
		}
	}
	
	/**
	 * Stop polling.
	 */
	@Override
	public void postStop() {
		System.out.println("Shutting down executive.");
		poll.cancel();
	}
	
	/**
	 * This message is sent to the executive by the scheduler every minute
	 * to poll the workers for their statuses.
	 * @author matthewc
	 *
	 */
	private static class Poll { /* nothing */ }
	
	/**
	 * This represents a single unit of work sent to a worker manager.
	 */
	private static class MultipointJobComponent {
		/** The job ID */
		public final int jobId;
		
		/** The from location */
		public final PointFeature from;
		
		/** The time this was sent */
		public final long sentTime;
		
		public MultipointJobComponent (int jobId, PointFeature from, long sentTime) {
			this.jobId = jobId;
			this.from = from;
			this.sentTime = sentTime;
		}
		
		public int hashCode () {
			// time not included as it's not included in equality.
			return jobId + from.getId().hashCode();
		}
		
		public boolean equals(Object o) {
			if (o instanceof MultipointJobComponent) {
				MultipointJobComponent c = (MultipointJobComponent) o;
				// we intentionally don't include time, as when a request comes back we don't know nor
				// care what time it was sent.
				return c.jobId == this.jobId && c.from.getId().equals(this.from.getId());
			}
			return false;
		}
		
		public String toString () {
			return "job component, job " + jobId + ", feature " + from.getId();
		}
	}
}
