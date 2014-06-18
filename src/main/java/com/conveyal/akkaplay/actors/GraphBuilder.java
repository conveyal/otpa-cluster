package com.conveyal.akkaplay.actors;

import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.message.BuildGraph;

import akka.actor.UntypedActor;

public class GraphBuilder extends UntypedActor {
	
	AmazonS3 s3;
	
	GraphBuilder(){
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials(); //grab credentials from "~.aws/credentials"
		s3 = new AmazonS3Client(creds);

	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof BuildGraph){
			BuildGraph bg = (BuildGraph)msg;
			
			ObjectListing ol = this.s3.listObjects(bg.bucket);
			for( S3ObjectSummary os : ol.getObjectSummaries() ){
				System.out.println( os.getKey() );
			}
			
//			URL gtfs_url = new URL("https://s3.amazonaws.com"+bg.gtfs_path);
//			System.out.println( gtfs_url );
//			URL osm_url = new URL("https://s3.amazonaws.com"+bg.osm_path);
//			
//			System.out.println( "getting gtfs" );
//			ReadableByteChannel rbc = Channels.newChannel(gtfs_url.openStream());
//			FileOutputStream fos = new FileOutputStream(UUID.randomUUID().toString()+".zip");
//			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
//			
//			System.out.println( "getting osm" );
//			rbc = Channels.newChannel(osm_url.openStream());
//			fos = new FileOutputStream(UUID.randomUUID().toString()+".osm.pbf");
//			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
//			
//			System.out.println( "start building graph gtfs:"+bg.gtfs_path+" osm:"+bg.osm_path );
			
			
		}
	}

}
