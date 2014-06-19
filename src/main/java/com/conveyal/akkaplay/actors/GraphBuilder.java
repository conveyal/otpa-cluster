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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.message.BuildGraph;

import akka.actor.UntypedActor;

public class GraphBuilder extends UntypedActor {

	AmazonS3 s3;

	GraphBuilder() {
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials(); // grab
																					// credentials
																					// from
																					// "~.aws/credentials"
		s3 = new AmazonS3Client(creds);

	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof BuildGraph) {
			BuildGraph bg = (BuildGraph) msg;

			ObjectListing ol = s3.listObjects(bg.bucket);
			for (S3ObjectSummary os : ol.getObjectSummaries()) {
				System.out.println("getting " + os.getKey());
				S3Object obj = s3.getObject(bg.bucket, os.getKey());
				InputStream objectData = obj.getObjectContent();

				String key = os.getKey();
				if(key.charAt(key.length()-1)!='/'){
					String filename = "tmp/" + bg.bucket + "/" + key;
					saveFile(filename, objectData);
				}
			}

			// URL gtfs_url = new URL("https://s3.amazonaws.com"+bg.gtfs_path);
			// System.out.println( gtfs_url );
			// URL osm_url = new URL("https://s3.amazonaws.com"+bg.osm_path);
			//
			// System.out.println( "getting gtfs" );
			// ReadableByteChannel rbc =
			// Channels.newChannel(gtfs_url.openStream());
			// FileOutputStream fos = new
			// FileOutputStream(UUID.randomUUID().toString()+".zip");
			// fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			//
			// System.out.println( "getting osm" );
			// rbc = Channels.newChannel(osm_url.openStream());
			// fos = new
			// FileOutputStream(UUID.randomUUID().toString()+".osm.pbf");
			// fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			//
			// System.out.println(
			// "start building graph gtfs:"+bg.gtfs_path+" osm:"+bg.osm_path );

		}
	}

	private void saveFile(String filename, InputStream inputStream) throws IOException {
		OutputStream outputStream=null;
		
		try {

			// write the inputStream to a FileOutputStream
			File ff = new File(filename);
			ff.getParentFile().mkdirs();
			ff.createNewFile();
			outputStream = new FileOutputStream(ff);

			int read = 0;
			byte[] bytes = new byte[1024];

			while ((read = inputStream.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
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
		}
	}

}
