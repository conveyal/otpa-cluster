package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.glassfish.grizzly.websockets.WebSocketApplication;

import com.conveyal.akkaplay.JobResultsApplication;
import com.conveyal.akkaplay.StatusServer;
import com.conveyal.akkaplay.message.*;

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
	Map<ActorSelection,Integer> managers;
	Map<Integer, ActorRef> jobManagers;
	JobResultsApplication statusServer = null;
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	Executive() {
		jobResults = new HashMap<Integer, ArrayList<WorkResult>>();

		managers = new HashMap<ActorSelection,Integer>();
		
		jobManagers = new HashMap<Integer,ActorRef>();

	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof JobSpec) {
			// if there are no workers to route to, bail
			if (managers.size() == 0) {
				getSender().tell(new JobId(-1), getSelf());
				return;
			}

			// the executive gives jobs ids
			JobSpec jobSpec = (JobSpec) msg;
			jobSpec.jobId = jobId;

			// make a place to catch the results of the job
			jobResults.put(jobId, new ArrayList<WorkResult>());

			// send the job id to the client
			getSender().tell(new JobId(jobId), getSelf());
			
			// create a job manager
			ActorRef jobManager = getContext().actorOf(Props.create(JobManager.class), "jobmanager-"+jobId);
			jobManagers.put( jobId, jobManager );
			
			// assign some managers to the job manager
			for(ActorSelection manager : freeManagers() ){
				assignManager( jobId, manager );
			}
			
			// kick off the job
			jobManager.tell( jobSpec, getSelf() );

			jobId += 1;
		} else if (msg instanceof WorkResult) {
			WorkResult wr = (WorkResult) msg;
			
			jobResults.get(wr.jobId).add(wr);

			//log.debug("work result got: {}", wr);
			if(statusServer!=null){
				statusServer.onWorkResult( wr );
			}

		} else if (msg instanceof JobResultQuery) {
			JobResultQuery jr = (JobResultQuery) msg;
			ArrayList<WorkResult> res = jobResults.get(jr.jobId);
			getSender().tell(new JobResult(res), getSelf());
		} else if (msg instanceof AddManager) {
			AddManager aw = (AddManager) msg;
			System.out.println("add worker " + aw.remote);

			aw.remote.tell(new AssignExecutive(), getSelf());

			managers.put(aw.remote,null);
		} else if (msg instanceof JobStatusQuery) {
			ArrayList<JobStatus> ret = new ArrayList<JobStatus>();
			for (ActorSelection manager : managers.keySet()) {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(manager, new JobStatusQuery(), timeout);
				JobStatus result = (JobStatus) Await.result(future, timeout.duration());
				ret.add(result);
			}
			getSender().tell(ret, getSelf());
		} else if (msg instanceof JobDone){
			JobDone jd = (JobDone)msg;
			
			// free up managers
			for(ActorSelection manager : jd.managers){
				managers.put(manager, null);
			}
			
			// deallocate job manager
			jobManagers.put(jd.jobId, null);
			getContext().system().stop(getSender());
			
			log.debug("{} says job done", getSender());
		} else if (msg instanceof SetStatusServer){
			this.statusServer = ((SetStatusServer)msg).statusServer;
		} else if (msg instanceof Terminated) {
			// router = router.removeRoutee(((Terminated) msg).actor());
			// ActorRef r = getContext().actorOf(Props.create(Manager.class));
			// getContext().watch(r);
			// router = router.addRoutee(new ActorRefRoutee(r));
		}
	}

	private boolean assignManager(int jobId, ActorSelection manager) throws Exception {
		// get the job manager for this job id
		ActorRef jobManager = jobManagers.get(jobId);
		
		// assign the manager to the job manager; blocking operation
		Timeout timeout = new Timeout(Duration.create(1, "seconds"));
		Future<Object> future = Patterns.ask(jobManager, new AddManager(manager), timeout);
		Boolean success = (Boolean) Await.result(future, timeout.duration());
		
		// if it worked, register the manager as busy
		if( success ){
			this.managers.put(manager, jobId);
		}
		
		return success;
	}

	private ArrayList<ActorSelection> freeManagers() {
		ArrayList<ActorSelection> ret = new ArrayList<ActorSelection>();
		
		for( Entry<ActorSelection,Integer> entry : this.managers.entrySet() ){
			if(entry.getValue()==null){
				ret.add( entry.getKey() );
			}
		}
		
		return ret;
	}

}
