package com.conveyal.akkaplay.actors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

import org.opentripplanner.graph_builder.GraphBuilderTask;
import org.opentripplanner.routing.graph.Graph;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.AnalystGraphBuilder;
import com.conveyal.akkaplay.Util;
import com.conveyal.akkaplay.message.BuildGraph;

import akka.actor.UntypedActor;

public class GraphBuilder extends UntypedActor {

	AmazonS3 s3;
	static String sourceDir="graphsource";

	GraphBuilder() {
		// grab credentials from "~.aws/credentials"
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials();
		s3 = new AmazonS3Client(creds);

	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof BuildGraph) {
			BuildGraph bg = (BuildGraph) msg;

			if( !bucketCached(bg.bucket) ) {
				System.out.println( "downloading graph sources" );
				downloadGraphSourceFiles(bg.bucket,sourceDir);
			} else {
				System.out.println( "graph sources already cached" );
			}
			
			Graph gg = buildGraphToMemory(bg.bucket);
			
			getSender().tell(gg, getSelf());
		}
	}

	private boolean bucketCached(String bucket) {
		File bucketDir = new File(sourceDir+"/"+bucket);
		return bucketDir.exists();
	}

	private Graph buildGraphToMemory(String bucket) {
		GraphBuilderTask gbt = AnalystGraphBuilder.createBuilder(new File(sourceDir+"/"+bucket) );
		gbt.setSerializeGraph(false);
		gbt.setPath(new File("bogus")); //will never be used, because serialize set to false
		gbt.run();
		Graph gg = gbt.getGraph();
		return gg;
	}

	private void downloadGraphSourceFiles(String bucket, String dirName) throws IOException {
		ObjectListing ol = s3.listObjects(bucket);
		for (S3ObjectSummary os : ol.getObjectSummaries()) {
			System.out.println("getting " + os.getKey());
			S3Object obj = s3.getObject(bucket, os.getKey());
			InputStream objectData = obj.getObjectContent();

			String key = os.getKey();
			if (key.charAt(key.length() - 1) != '/') {
				String filename = dirName + "/" + bucket + "/" + key;
				Util.saveFile(filename, objectData, os.getSize(), true);
			}
		}
	}



}
