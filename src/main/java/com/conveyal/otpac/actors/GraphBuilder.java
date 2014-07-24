package com.conveyal.otpac.actors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.zip.ZipUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.graph_builder.GraphBuilderTask;
import org.opentripplanner.routing.graph.Graph;

import ch.qos.logback.core.util.FileUtil;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.otpac.ClusterGraphService;
import com.conveyal.otpac.message.BuildGraph;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class GraphBuilder extends UntypedActor {

	private ClusterGraphService graphService;
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof BuildGraph) {
			onMsgBuildGraph((BuildGraph) msg);
		}
	}

	public GraphBuilder(Boolean workOffline) {
		graphService = new ClusterGraphService(context().system().settings().config().getString("s3.credentials.filename"), workOffline);
	}
	
	private void onMsgBuildGraph(BuildGraph bg) throws IOException {
		
		Graph gg = graphService.getGraph(bg.graphId);

		getSender().tell(gg, getSelf());
	}

	
}
