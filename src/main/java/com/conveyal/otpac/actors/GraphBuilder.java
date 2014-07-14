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

	Boolean workOffline = false;
	
	GraphBuilder(Boolean workOffline) {
		
		if(!workOffline) {
			String s3ConfigFilename = context().system().settings().config().getString("s3.credentials.filename");
			AWSCredentials creds = new ProfileCredentialsProvider(s3ConfigFilename, "default").getCredentials();
			s3 = new AmazonS3Client(creds);
		}
		
		this.workOffline = workOffline;
	}
	
	// adds either a zip file or graph directory to S3, or local cache for offline use
	public void addGraphFile(File graphFile) throws IOException {
		
		String graphId = graphFile.getName();
		
		if(graphId.endsWith(".zip"))
			graphId = graphId.substring(0, graphId.length() - 5);
		
		File graphDataZip = new File(sourceDir + "/" + graphId + ".zip");
				
		// if directory zip contents  store as zip
		if(graphFile.isDirectory()) {
			zipGraphDir(graphFile, graphDataZip);
		}
		else if(graphFile.getName().endsWith(".zip")) {
			FileUtils.copyFile(graphFile, graphDataZip);
		}
		else {
			log.debug("unknown graph input format, unable to store");
			graphDataZip = null;
		}
			
		if(!workOffline && graphDataZip != null) {
			PutObjectResult graphZip = s3.putObject(graphBucket, graphId+".zip", graphDataZip);
		}
		
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof BuildGraph) {
			onMsgBuildGraph((BuildGraph) msg);
		}
	}

	private void onMsgBuildGraph(BuildGraph bg) throws IOException {
		
		
		if (!bucketCached(bg.graphId)) {
			if(!workOffline) {
				log.debug("downloading graph sources");
				downloadGraphSourceFiles(bg.graphId, sourceDir);
			}
			else
				log.debug("graph not cached for offline use");
		} else {
			log.debug("graph sources already cached");
		}

		Graph gg = buildGraphToMemory(bg.graphId);

		getSender().tell(gg, getSelf());
	}

	private static boolean bucketCached(String graphId) throws IOException {
		File graphData = new File(sourceDir + "/" + graphId);
		
		// check if cached but only as zip
		if(!graphData.exists()) {
			File graphDataZip = new File(sourceDir + "/" + graphId + ".zip");
			
			if(graphDataZip.exists()) {
				zipGraphDir(graphData, graphDataZip);
			}
		}
		
		
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

		unpackGraphZip(graphZipFile, extractedGraphDir);
	}

	private static void unpackGraphZip(File graphZipFile, File extractedGraphDir) throws ZipException, IOException {
		
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
	
	private static void zipGraphDir(File graphDirectory, File zipGraphFile) throws IOException {
		
		FileOutputStream fileOutputStream = new FileOutputStream(zipGraphFile);
		ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream);
		
		byte[] buffer = new byte[1024];
		
		for(File f : graphDirectory.listFiles()) {
			ZipEntry zipEntry = new ZipEntry(f.getName());
			zipOutputStream.putNextEntry(zipEntry);
    		FileInputStream fileInput = new FileInputStream(f);

    		int len;
    		while ((len = fileInput.read(buffer)) > 0) {
    			zipOutputStream.write(buffer, 0, len);
    		}
 
    		fileInput.close();
    		zipOutputStream.closeEntry();
		}
		
		zipOutputStream.close();
	}
}
