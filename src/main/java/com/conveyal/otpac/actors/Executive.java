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
	Map<String,ActorRef> wmPathActorRefs; //path->canonical actorref
	Map<String, Integer> workerManagers; //path->jobid
	Map<Integer, ActorRef> jobManagers;
	
	Boolean workOffline;

	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	Executive() {
		this(false);
	}
	
	Executive(Boolean workOffline) {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		workerManagers = new HashMap<String, Integer>();
		wmPathActorRefs = new HashMap<String,ActorRef>();

		jobManagers = new HashMap<Integer, ActorRef>();

		this.workOffline = workOffline;
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
		} else if (msg instanceof CancelJob) {
			onMsgCancelJob((CancelJob) msg);
		} else if (msg instanceof Terminated) {
			onMsgTerminated();
		} else if(msg instanceof String){
			getSender().tell(msg, getSelf());
		} 
	}

	private void onMsgCancelJob(CancelJob msg) {
		
		ActorRef jm = getJobManager( msg.jobid );
		if(jm==null){
			return;
		}
		
		jm.tell( new CancelJob(), getSelf() );
		
	}

	private void onMsgTerminated() {
		ActorRef dead = getSender();
		
		// if the workermanager is assigned to a jobmanager, tell the jobmanager to remove it
		Integer jobId = getWorkerManagerJob( dead );
		if(jobId != null){
			getJobManager(jobId).tell( new RemoveWorkerManager(dead), getSelf() );
			freeWorkerManager(dead.path().toString()); //TODO double check this
		}
		
		// delete the workermanager from the roster
		deleteWorkerManager(dead);
	}

	private void deleteWorkerManager(ActorRef dead) {
		this.workerManagers.remove(dead.path().toString());
	}

	private ActorRef getJobManager(Integer jobId) {
		return this.jobManagers.get(jobId);
	}

	private Integer getWorkerManagerJob(ActorRef dead) {
		return this.workerManagers.get(dead.path().toString());
	}

	private void onMsgJobDone(JobDone jd) {
		// free up WorkerManagers
		for (ActorRef workerManager : jd.workerManagers) {
			freeWorkerManager(workerManager.path().toString());
		}

		// deallocate job manager
		jobManagers.put(jd.jobId, null);
		getContext().system().stop(getSender());

		if( jd.status == JobDone.Status.SUCCESS ){
			log.debug("{} says job {} done", getSender(), jd.jobId);
		} else if (jd.status == JobDone.Status.CANCELLED ){
			log.debug("{} says job {} cancelled", getSender(), jd.jobId);
		}
	}

	private void freeWorkerManager(String workerManagerPath) {
		workerManagers.put(workerManagerPath, null);
	}

	private void onMsgJobStatusQuery() throws Exception {
		ArrayList<JobStatus> ret = new ArrayList<JobStatus>();

		for (String workerManagerPath : workerManagers.keySet()) {
			ActorRef workerManager = this.wmPathActorRefs.get( workerManagerPath );
			
			Timeout timeout = new Timeout(Duration.create(60, "seconds"));
			Future<Object> future = Patterns.ask(workerManager, new JobStatusQuery(), timeout);
			JobStatus result = (JobStatus) Await.result(future, timeout.duration());
			ret.add(result);
		}
		getSender().tell(ret, getSelf());
	}

	private void onMsgAddWorkerManager(AddWorkerManager aw) throws Exception {
		ActorRef remoteManager = aw.workerManager;
		
		this.wmPathActorRefs.put(remoteManager.path().toString(), remoteManager);
		
		// watch for termination
		getContext().watch(remoteManager);
		
		System.out.println("add worker " + remoteManager);

		Timeout timeout = new Timeout(Duration.create(60, "seconds"));
		Future<Object> future = Patterns.ask(remoteManager, new AssignExecutive(), timeout);
		Await.result( future, timeout.duration() );
		remoteManager.tell(new AssignExecutive(), getSelf());
		
		workerManagers.put(remoteManager.path().toString(), null);
		
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
		ActorRef jobManager = getContext().actorOf(Props.create(JobManager.class, workOffline), "jobmanager-" + jobId);
		
		jobManagers.put(jobId, jobManager);

		// assign some managers to the job manager
		for (String manager : getFreeWorkerManagers()) {
			assignWorkerManager(jobId, manager);
		}

		// kick off the job
		jobManager.tell(jobSpec, getSelf());

		jobId += 1;
	}

	private boolean assignWorkerManager(int jobId, String managerPath) throws Exception {
		// get the job manager for this job id
		ActorRef jobManager = jobManagers.get(jobId);
		
		ActorRef workerManager = wmPathActorRefs.get( managerPath );

		// assign the workermanager to the jobmanager; blocking operation
		Timeout timeout = new Timeout(Duration.create(60, "seconds"));
		Future<Object> future = Patterns.ask(jobManager, new AssignWorkerManager(workerManager), timeout);

		Boolean success = (Boolean) Await.result(future, timeout.duration());

		// if it worked, register the manager as busy
		if (success) {
			this.workerManagers.put(managerPath, jobId);
		}

		return success;
	}

	private ArrayList<String> getFreeWorkerManagers() {
		ArrayList<String> ret = new ArrayList<String>();

		for (Entry<String, Integer> entry : this.workerManagers.entrySet()) {
			if (entry.getValue() == null) {
				ret.add(entry.getKey());
			}
		}

		return ret;
	}

}
