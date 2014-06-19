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

			//downloadGraphSourceFiles(bg.bucket,sourceDir);
			
			GraphBuilderTask gbt = AnalystGraphBuilder.createBuilder(new File(sourceDir+"/"+bg.bucket) );
			gbt.setSerializeGraph(false);
			gbt.setPath(new File("bogus")); //will never be used, because serialize set to false
			gbt.run();
			Graph gg = gbt.getGraph();
			System.out.println( "graph built nvertices="+gg.countVertices()+" nedges="+gg.countVertices() );
		}
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
				saveFile(filename, objectData, os.getSize(), true);
			}
		}
	}

	private void saveFile(String filename, InputStream inputStream, long size, boolean verbose) throws IOException {
		OutputStream outputStream = null;

		try {

			// write the inputStream to a FileOutputStream
			File ff = new File(filename);
			ff.getParentFile().mkdirs();
			ff.createNewFile();
			outputStream = new FileOutputStream(ff);

			int read = 0;
			byte[] bytes = new byte[1024];
			int totalRead = 0;

			while ((read = inputStream.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
				totalRead += read;
				if (verbose) {
					System.out.print("\r" + totalRead + "/" + size);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (outputStream != null) {
				// outputStream.flush();
				outputStream.close();

			}

			if (verbose) {
				System.out.print("\n");
			}
		}
	}

}
