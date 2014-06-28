package com.conveyal.akkaplay.actors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.util.DateUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.Util;
import com.conveyal.akkaplay.message.AddManager;
import com.conveyal.akkaplay.message.JobDone;
import com.conveyal.akkaplay.message.JobSliceDone;
import com.conveyal.akkaplay.message.JobSliceSpec;
import com.conveyal.akkaplay.message.JobSpec;
import com.conveyal.akkaplay.message.WorkResult;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class JobManager extends UntypedActor {

	private ArrayList<ActorSelection> managers;
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	int workersOut=0;
	
	AmazonS3 s3;
	private ActorRef executive;
	private int jobId;

	JobManager() {
		// grab credentials from "~.aws/credentials"
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials();
		s3 = new AmazonS3Client(creds);
		
		managers = new ArrayList<ActorSelection>();
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof AddManager) {
			AddManager am = (AddManager) msg;
			managers.add(am.remote);
			getSender().tell(new Boolean(true), getSelf());
		} else if (msg instanceof JobSpec) {
			JobSpec js = (JobSpec)msg;
			
			this.jobId = js.jobId;
			
			// bond to the executive that sent this
			this.executive = getSender();
			
			log.debug( "get origin pointset: {}",js.fromPtsLoc );
			PointSet fromPts = getPointset( js.fromPtsLoc );
			log.debug( "got origin pointset: {}",fromPts.featureCount() );
			
			log.debug( "get destination pointset: {}",js.toPtsLoc );
			PointSet toPts = getPointset( js.toPtsLoc );
			log.debug( "got destination pointset: {}",toPts.featureCount() );
			
			TimeZone tz = TimeZone.getTimeZone(js.tz);
			Date date = DateUtils.toDate(js.date, js.time, tz);

			// split the job evenly between managers
			float seglen = fromPts.featureCount() / ((float) managers.size());
			for(int i=0;i<managers.size(); i++){				
				int start = Math.round(seglen * i);
				int end = Math.round(seglen * (i + 1));
				
				PointSet fromSplit = fromPts.slice(start, end);
				ActorSelection manager = managers.get(i);
				
				workersOut+=1;
				manager.tell(new JobSliceSpec(fromSplit,toPts,js.graphId,date), getSelf());
			}
		} else if(msg instanceof WorkResult){
			WorkResult res = (WorkResult)msg;
			res.jobId = jobId;
						
			executive.tell(res, getSelf());
		} else if(msg instanceof JobSliceDone){
			JobSliceDone doneMsg = (JobSliceDone)msg;
			
			workersOut-=1;
			log.debug("worker {} is done", getSender());
			
			if (workersOut==0){
				executive.tell(new JobDone(jobId, managers), getSelf());
			}
		}
	}

	private PointSet getPointset(String ptsLoc) throws Exception {

		// get pointset metadata from S3
		S3Object obj = s3.getObject("pointsets",ptsLoc);
		ObjectMetadata objMet = obj.getObjectMetadata();
		
		// if it's not already cached, do so
		String objEtag = objMet.getETag();
		File cachedFile = new File("cache/"+objEtag+"-"+ptsLoc);
		if(!cachedFile.exists()){
			log.debug("caching pointset: {}", ptsLoc);
			Util.saveFile( cachedFile, obj.getObjectContent(), objMet.getContentLength(), true);
		}
		
		// grab it from the cache
		InputStream objectData = new FileInputStream( cachedFile );
		
		PointSet ret=null;
		if( isCsv(ptsLoc) ){
			ret = PointSet.fromCsv( "cache/"+objEtag+"-"+ptsLoc );
		}
		
		objectData.close();
		return ret;

	}

	private boolean isCsv(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		String formatPart = parts[parts.length-1];
		return formatPart.equals("csv");
	}

}
