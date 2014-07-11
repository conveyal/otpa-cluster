package com.conveyal.otpac.actors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.graph_builder.GraphBuilderTask;
import org.opentripplanner.routing.graph.Graph;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.conveyal.otpac.AnalystGraphBuilder;
import com.conveyal.otpac.message.BuildGraph;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class GraphBuilder extends UntypedActor {

	AmazonS3 s3;
	static String sourceDir = "graphsource";

	// TODO fix hard coded bucket name (config on boot? add to message?)
	static String graphBucket = "otpac-graphs";
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	GraphBuilder() {
		String s3ConfigFilename = context().system().settings().config().getString("s3.credentials.filename");
		AWSCredentials creds = new ProfileCredentialsProvider(s3ConfigFilename, "default").getCredentials();
		s3 = new AmazonS3Client(creds);

	}

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof BuildGraph) {
			onMsgBuildGraph((BuildGraph) msg);
		}
	}

	private void onMsgBuildGraph(BuildGraph bg) throws IOException {
		if (!bucketCached(bg.graphId)) {
			log.debug("downloading graph sources");
			downloadGraphSourceFiles(bg.graphId, sourceDir);
		} else {
			log.debug("graph sources already cached");
		}

		Graph gg = buildGraphToMemory(bg.graphId);

		getSender().tell(gg, getSelf());
	}

	private boolean bucketCached(String graphId) {
		File graphData = new File(sourceDir + "/" + graphId);
		return graphData.exists() && graphData.isDirectory();
	}

	private Graph buildGraphToMemory(String graphId) {
		GraphBuilderTask gbt = AnalystGraphBuilder.createBuilder(new File(sourceDir + "/" + graphId));
		gbt.setSerializeGraph(false);
		gbt.setPath(new File("bogus")); // will never be used, because serialize
										// set to false
		gbt.run();
		Graph gg = gbt.getGraph();
		return gg;
	}

	private void downloadGraphSourceFiles(String graphId, String dirName) throws IOException {

		File graphCacheDir = new File(dirName);
		if (!graphCacheDir.exists())
			graphCacheDir.mkdirs();

		File graphZipFile = new File(graphCacheDir, graphId + ".zip");

		File extractedGraphDir = new File(graphCacheDir, graphId);

		if (extractedGraphDir.exists()) {
			FileUtils.deleteDirectory(extractedGraphDir);
		}

		extractedGraphDir.mkdirs();

		log.info( "downloading s3 object bucket:{} key:{}", graphBucket, graphId+".zip");
		S3Object graphZip = s3.getObject(graphBucket, graphId+".zip");

		InputStream zipFileIn = graphZip.getObjectContent();

		OutputStream zipFileOut = new FileOutputStream(graphZipFile);

		IOUtils.copy(zipFileIn, zipFileOut);
		IOUtils.closeQuietly(zipFileIn);
		IOUtils.closeQuietly(zipFileOut);

		ZipFile zipFile = new ZipFile(graphZipFile);

		Enumeration<? extends ZipEntry> entries = zipFile.entries();

		while (entries.hasMoreElements()) {

			ZipEntry entry = entries.nextElement();
			File entryDestination = new File(extractedGraphDir, entry.getName());

			entryDestination.getParentFile().mkdirs();

			if (entry.isDirectory())
				entryDestination.mkdirs();
			else {
				InputStream entryFileIn = zipFile.getInputStream(entry);
				OutputStream entryFileOut = new FileOutputStream(entryDestination);
				IOUtils.copy(entryFileIn, entryFileOut);
				IOUtils.closeQuietly(entryFileIn);
				IOUtils.closeQuietly(entryFileOut);
			}
		}

		zipFile.close();

		graphZipFile.delete();
	}

}
