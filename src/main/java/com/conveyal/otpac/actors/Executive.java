package com.conveyal.otpac.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.conveyal.otpac.message.*;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
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
	Map<ActorSelection, Integer> managers;
	Map<Integer, ActorRef> jobManagers;

	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	Executive() {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		managers = new HashMap<ActorSelection, Integer>();

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
		} else if (msg instanceof AddManager) {
			onMsgAddManager((AddManager) msg);
		} else if (msg instanceof JobStatusQuery) {
			onMsgJobStatusQuery();
		} else if (msg instanceof JobDone) {
			onMsgJobDone((JobDone) msg);
		} else if (msg instanceof Terminated) {
			// router = router.removeRoutee(((Terminated) msg).actor());
			// ActorRef r = getContext().actorOf(Props.create(Manager.class));
			// getContext().watch(r);
			// router = router.addRoutee(new ActorRefRoutee(r));
		}
	}

	private void onMsgJobDone(JobDone jd) {
		// free up managers
		for (ActorSelection manager : jd.managers) {
			managers.put(manager, null);
		}

		// deallocate job manager
		jobManagers.put(jd.jobId, null);
		getContext().system().stop(getSender());

		log.debug("{} says job done", getSender());
	}

	private void onMsgJobStatusQuery() throws Exception {
		ArrayList<JobStatus> ret = new ArrayList<JobStatus>();
		for (ActorSelection manager : managers.keySet()) {
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> future = Patterns.ask(manager, new JobStatusQuery(), timeout);
			JobStatus result = (JobStatus) Await.result(future, timeout.duration());
			ret.add(result);
		}
		getSender().tell(ret, getSelf());
	}

	private void onMsgAddManager(AddManager aw) throws Exception {
		System.out.println("add worker " + aw.remote);

		// make sure we can reach the remote WorkerManager
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(aw.remote, new AssignExecutive(), timeout);
		Boolean result = (Boolean)Await.result( future, timeout.duration() );
		if(result){
			log.info("connected remote manager {}", aw.remote);
		} else {
			log.info("something went wrong connecting to manager {}", aw.remote);
		}

		managers.put(aw.remote, null);
		
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
		if (managers.size() == 0) {
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
		for (ActorSelection manager : freeManagers()) {
			assignManager(jobId, manager);
		}

		// kick off the job
		jobManager.tell(jobSpec, getSelf());

		jobId += 1;
	}

	private boolean assignManager(int jobId, ActorSelection manager) throws Exception {
		// get the job manager for this job id
		ActorRef jobManager = jobManagers.get(jobId);

		// assign the manager to the job manager; blocking operation
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(jobManager, manager, timeout);
		Boolean success = (Boolean) Await.result(future, timeout.duration());

		// if it worked, register the manager as busy
		if (success) {
			this.managers.put(manager, jobId);
		}

		return success;
	}

	private ArrayList<ActorSelection> freeManagers() {
		ArrayList<ActorSelection> ret = new ArrayList<ActorSelection>();

		for (Entry<ActorSelection, Integer> entry : this.managers.entrySet()) {
			if (entry.getValue() == null) {
				ret.add(entry.getKey());
			}
		}

		return ret;
	}

}
