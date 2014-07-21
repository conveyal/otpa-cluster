package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.util.DateUtils;

import com.conveyal.otpac.S3Datastore;
import com.conveyal.otpac.message.JobDone;
import com.conveyal.otpac.message.JobSliceDone;
import com.conveyal.otpac.message.JobSliceSpec;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.RemoveWorkerManager;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.JobItemCallback;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class JobManager extends UntypedActor {

	private Set<ActorRef> workerManagersReady;
	private Set<ActorRef> workerManagersOut;
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	
	private ActorRef executive;
	private int jobId;
	private JobItemCallback callback;
	private S3Datastore s3Store;

	JobManager() {
		String s3ConfigFilename = context().system().settings().config().getString("s3.credentials.filename");
		
		s3Store = new S3Datastore(s3ConfigFilename);
		
		workerManagersReady = new HashSet<ActorRef>();
		workerManagersOut = new HashSet<ActorRef>();
	}

	@Override
	public void onReceive(Object msg) throws Exception {		
		if (msg instanceof ActorRef) {
			onMsgActorRef((ActorRef) msg);
		} else if (msg instanceof JobSpec) {
			onMsgJobSpec((JobSpec) msg);
		} else if(msg instanceof WorkResult){
			onMsgWorkResult((WorkResult) msg);
		} else if(msg instanceof JobSliceDone){			
			onMsgJobSliceDone();
		} else if(msg instanceof RemoveWorkerManager){
			onMsgRemoveWorkerManager((RemoveWorkerManager)msg);
		} else if(msg instanceof Terminated){
			unhandled(msg);
		} else {
			unhandled(msg);
		}
	}

	private void onMsgRemoveWorkerManager(RemoveWorkerManager msg) {
		ActorRef dead = msg.workerManager;
		
		if( workerManagersOut.contains( dead ) ){
			workerManagersOut.remove( dead );
			cancelAndReturn();
			return;
		}
		
		boolean removedFromReady = workerManagersReady.remove(dead);
		
		if(!removedFromReady){
			log.error("attepting to remove workermanager not on the jobmanager's roster");
			//TODO raise error more sternly?
		}	
	}

	private void cancelAndReturn() {
		// cancel every WorkerManager still on a job
		// move them all the the ready set
		// report to supervisor
	}

	private void onMsgJobSliceDone() {
		ActorRef mng = getSender();
		workerManagersOut.remove(mng);
		workerManagersReady.add(mng);
		
		if (workerManagersOut.isEmpty()){
			executive.tell(new JobDone(jobId, workerManagersReady), getSelf());
		}
	}

	private void onMsgWorkResult(WorkResult res) throws IOException {
		res.jobId = jobId;
		
		if(callback != null){
			this.callback.onWorkResult( res );
		}
					
		executive.tell(res, getSelf());
	}

	private void onMsgJobSpec(JobSpec js) throws Exception {		
		this.jobId = js.jobId;
		this.callback = js.callback;
		
		// bond to the executive that sent this
		this.executive = getSender();
		
		log.debug( "get origin pointset: {}",js.fromPtsLoc );
		PointSet fromPts = s3Store.getPointset( js.fromPtsLoc );
		log.debug( "got origin pointset: {}",fromPts.featureCount() );
		
		TimeZone tz = TimeZone.getTimeZone(js.tz);
		Date date = DateUtils.toDate(js.date, js.time, tz);

		// split the job evenly between managers
		float seglen = fromPts.featureCount() / ((float) workerManagersReady.size());
		int i=0;
		
		for(ActorRef workerManager : workerManagersReady){
			int start = Math.round(seglen * i);
			int end = Math.round(seglen * (i + 1));
									
			workerManager.tell(new JobSliceSpec(js.fromPtsLoc,start,end,js.toPtsLoc,js.graphId,date), getSelf());
			
			workerManagersOut.add( workerManager );
			
			i++;
		}
		
		// since we added every WorkerManager in the ready set to the out set, we can empty it
		workerManagersReady.clear();
		
	}

	private void onMsgActorRef(ActorRef asel) {
		//getContext().watch(asel);
		
		workerManagersReady.add(asel);
		getSender().tell(new Boolean(true), getSelf());
	}



}
