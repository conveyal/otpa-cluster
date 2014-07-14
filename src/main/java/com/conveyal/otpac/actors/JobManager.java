package com.conveyal.otpac.actors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.util.DateUtils;

import com.conveyal.otpac.S3Datastore;
import com.conveyal.otpac.message.AddManager;
import com.conveyal.otpac.message.JobDone;
import com.conveyal.otpac.message.JobSliceDone;
import com.conveyal.otpac.message.JobSliceSpec;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.WorkResult;
import com.conveyal.otpac.JobItemCallback;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class JobManager extends UntypedActor {

	private ArrayList<ActorSelection> managers;
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	int workersOut=0;
	
	private ActorRef executive;
	private int jobId;
	private JobItemCallback callback;
	private S3Datastore s3Store;

	JobManager() {
		String s3ConfigFilename = context().system().settings().config().getString("s3.credentials.filename");
		
		s3Store = new S3Datastore(s3ConfigFilename);
		
		managers = new ArrayList<ActorSelection>();
	}

	@Override
	public void onReceive(Object msg) throws Exception {		
		if (msg instanceof ActorSelection) {
			onMsgActorSelection((ActorSelection) msg);
		} else if (msg instanceof JobSpec) {
			onMsgJobSpec((JobSpec) msg);
		} else if(msg instanceof WorkResult){
			onMsgWorkResult((WorkResult) msg);
		} else if(msg instanceof JobSliceDone){			
			onMsgJobSliceDone();
		} else {
			unhandled(msg);
		}
	}

	private void onMsgJobSliceDone() {
		workersOut-=1;
		log.debug("worker {} is done", getSender());
		
		if (workersOut==0){
			executive.tell(new JobDone(jobId, managers), getSelf());
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
		float seglen = fromPts.featureCount() / ((float) managers.size());
		for(int i=0;i<managers.size(); i++){				
			int start = Math.round(seglen * i);
			int end = Math.round(seglen * (i + 1));
						
			ActorSelection manager = managers.get(i);
			
			workersOut+=1;
			manager.tell(new JobSliceSpec(js.fromPtsLoc,start,end,js.toPtsLoc,js.graphId,date), getSelf());
		}
	}

	private void onMsgActorSelection(ActorSelection asel) {
		managers.add(asel);
		getSender().tell(new Boolean(true), getSelf());
	}



}
