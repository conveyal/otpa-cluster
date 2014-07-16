package com.conveyal.otpac.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.conveyal.otpac.message.*;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
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
import akka.util.Timeout;

public class Executive extends UntypedActor {

	int tasksOut;
	int jobId = 0;
	Map<Integer, ArrayList<WorkResult>> jobResults;
	Map<ActorRef, Integer> workerManagers;
	Map<Integer, ActorRef> jobManagers;

	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	Executive() {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		workerManagers = new HashMap<ActorRef, Integer>();

		jobManagers = new HashMap<Integer, ActorRef>();

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
			onMsgJobStatusQuery();
		} else if (msg instanceof JobDone) {
			onMsgJobDone((JobDone) msg);
		} else if (msg instanceof Terminated) {
			System.out.println("#############EXECUTIVE: TERMINATED#############");
			
			// router = router.removeRoutee(((Terminated) msg).actor());
			// ActorRef r = getContext().actorOf(Props.create(Manager.class));
			// getContext().watch(r);
			// router = router.addRoutee(new ActorRefRoutee(r));
		}
	}

	private void onMsgJobDone(JobDone jd) {
		// free up WorkerManagers
		for (ActorRef workerManager : jd.workerManagers) {
			workerManagers.put(workerManager, null);
		}

		// deallocate job manager
		jobManagers.put(jd.jobId, null);
		getContext().system().stop(getSender());

		log.debug("{} says job done", getSender());
	}

	private void onMsgJobStatusQuery() throws Exception {
		ArrayList<JobStatus> ret = new ArrayList<JobStatus>();
		for (ActorRef workerManager : workerManagers.keySet()) {
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> future = Patterns.ask(workerManager, new JobStatusQuery(), timeout);
			JobStatus result = (JobStatus) Await.result(future, timeout.duration());
			ret.add(result);
		}
		getSender().tell(ret, getSelf());
	}

	private void onMsgAddWorkerManager(AddWorkerManager aw) throws Exception {
		ActorSelection remoteManagerSel = context().system().actorSelection(aw.path);
		
		// get ActorRef of remote WorkerManager
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(remoteManagerSel, new Identify("1"), timeout);
		ActorIdentity actorId = (ActorIdentity)Await.result( future, timeout.duration() );
		ActorRef remoteManager = actorId.getRef();
		
		// watch for termination
		//getContext().watch(remoteManager);
		
		System.out.println("add worker " + remoteManager);

		// make sure we can reach the remote WorkerManager
		timeout = new Timeout(Duration.create(5, "seconds"));
		future = Patterns.ask(remoteManager, new AssignExecutive(), timeout);
		Boolean result = (Boolean)Await.result( future, timeout.duration() );
		if(result){
			log.info("connected remote manager {}", remoteManager);
		} else {
			log.info("something went wrong connecting to manager {}", remoteManager);
		}

		workerManagers.put(remoteManager, null);
		
		getSender().tell(new Boolean(true), getSelf());
	}

	private void onMsgJobResultQuery(JobResultQuery jr) {
		ArrayList<WorkResult> res = jobResults.get(jr.jobId);
		getSender().tell(new JobResult(res), getSelf());
	}

	private void onMsgWorkResult(WorkResult wr) {
		jobResults.get(wr.jobId).add(wr);
	}

	private void onMsgJobSpec(JobSpec jobSpec) throws Exception {
		// if there are no workers to route to, bail
		if (workerManagers.size() == 0) {
			getSender().tell(new JobId(-1), getSelf());
			return;
		}

		// the executive gives jobs ids
		jobSpec.jobId = jobId;

		// make a place to catch the results of the job
		jobResults.put(jobId, new ArrayList<WorkResult>());

		// send the job id to the client
		getSender().tell(new JobId(jobId), getSelf());

		// create a job manager
		ActorRef jobManager = getContext().actorOf(Props.create(JobManager.class), "jobmanager-" + jobId);
		
		jobManagers.put(jobId, jobManager);

		// assign some managers to the job manager
		for (ActorRef manager : getFreeWorkerManagers()) {
			assignManager(jobId, manager);
		}

		// kick off the job
		jobManager.tell(jobSpec, getSelf());

		jobId += 1;
	}

	private boolean assignManager(int jobId, ActorRef manager) throws Exception {
		// get the job manager for this job id
		ActorRef jobManager = jobManagers.get(jobId);

		// assign the manager to the job manager; blocking operation
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(jobManager, manager, timeout);
		Boolean success = (Boolean) Await.result(future, timeout.duration());

		// if it worked, register the manager as busy
		if (success) {
			this.workerManagers.put(manager, jobId);
		}

		return success;
	}

	private ArrayList<ActorRef> getFreeWorkerManagers() {
		ArrayList<ActorRef> ret = new ArrayList<ActorRef>();

		for (Entry<ActorRef, Integer> entry : this.workerManagers.entrySet()) {
			if (entry.getValue() == null) {
				ret.add(entry.getKey());
			}
		}

		return ret;
	}

}
