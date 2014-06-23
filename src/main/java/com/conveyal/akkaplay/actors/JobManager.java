package com.conveyal.akkaplay.actors;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.CsvPointset;
import com.conveyal.akkaplay.Pointset;
import com.conveyal.akkaplay.Util;
import com.conveyal.akkaplay.message.AddManager;
import com.conveyal.akkaplay.message.JobSliceSpec;
import com.conveyal.akkaplay.message.JobSpec;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

public class JobManager extends UntypedActor {

	private ArrayList<ActorSelection> managers;
	
	AmazonS3 s3;

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
			
			System.out.println( "get origin pointset: "+js.fromPtsLoc );
			Pointset fromPts = getPointset( js.fromPtsLoc );
			System.out.println( "got origin pointset: "+fromPts.size() );
			
			System.out.println( "get destination pointset: "+js.toPtsLoc );
			Pointset toPts = getPointset( js.toPtsLoc );
			System.out.println( "got destination pointset: "+toPts.size() );

			// split the job evenly between managers
			for(int i=0;i<managers.size(); i++){
				Pointset fromSplit = fromPts.split(managers.size(), i);
				ActorSelection manager = managers.get(i);
				manager.tell(new JobSliceSpec(fromSplit,toPts,js.bucket), getSelf());
			}
		}
	}

	private Pointset getPointset(String ptsLoc) throws Exception {

		S3Object obj = s3.getObject("pointsets",ptsLoc);
		InputStream objectData = obj.getObjectContent();
		
		System.out.println( ptsLoc );
		if( isCsv(ptsLoc) ){
			return CsvPointset.fromStream( objectData );
		} else {
			return null;
		}

	}

	private boolean isCsv(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		String formatPart = parts[parts.length-1];
		return formatPart.equals("csv");
	}

}
