package com.conveyal.otpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.graph_builder.GraphBuilderTask;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.graph.Graph.LoadLevel;
import org.opentripplanner.routing.services.GraphService;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;


public class ClusterGraphService implements GraphService { 

	static File GRAPH_DIR = new File("cache", "graphs");
	
	// TODO fix hard coded bucket name (config on boot? add to message?)
	static String graphBucket = "otpac-graphs";
	
	private Boolean workOffline = false;
	private AmazonS3Client s3;

	private ConcurrentHashMap<String,Graph> graphMap = new ConcurrentHashMap<String,Graph>();
	
	public synchronized Graph getGraph(String graphId) {
		
		GRAPH_DIR.mkdirs();
		
		if(!graphMap.containsKey(graphId)) {
			
			try {
				if (!bucketCached(graphId)) {
					if(!workOffline) {
						downloadGraphSourceFiles(graphId, GRAPH_DIR);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			GraphBuilderTask gbt = ClusterGraphBuilder.createBuilder(new File(GRAPH_DIR, graphId));
			
			gbt.run();
			
			Graph g = gbt.getGraph();
			
			g.routerId = graphId;
			
			graphMap.put(graphId,g);
					
		}
		
		return graphMap.get(graphId);
	}

	public ClusterGraphService(String s3CredentialsFilename) {
		this(s3CredentialsFilename, false);
	}

	public ClusterGraphService(String s3CredentialsFilename, Boolean workOffline) {
		
		if(!workOffline) {
			AWSCredentials creds = new ProfileCredentialsProvider(s3CredentialsFilename, "default").getCredentials();
			s3 = new AmazonS3Client(creds);
		}
		
		this.workOffline = workOffline;
	}
	
	// adds either a zip file or graph directory to S3, or local cache for offline use
	public void addGraphFile(File graphFile) throws IOException {
		
		String graphId = graphFile.getName();
		
		if(graphId.endsWith(".zip"))
			graphId = graphId.substring(0, graphId.length() - 5);
		
		GRAPH_DIR.mkdirs();
		
		File graphDataZip = new File(GRAPH_DIR, graphId + ".zip");
				
		// if directory zip contents  store as zip
		if(graphFile.isDirectory()) {
			zipGraphDir(graphFile, graphDataZip);
		}
		else if(graphFile.getName().endsWith(".zip")) {
			FileUtils.copyFile(graphFile, graphDataZip);
		}
		else {
			graphDataZip = null;
		}
			
		if(!workOffline && graphDataZip != null) {
			PutObjectResult graphZip = s3.putObject(graphBucket, graphId+".zip", graphDataZip);
		}
		
	}
	
	public synchronized File getZippedGraph(String graphId) throws IOException {
		
		File graphDataDir = new File(GRAPH_DIR, graphId);
		
		File graphZipFile = new File(GRAPH_DIR, graphId + ".zip");
		
		if(!graphDataDir.exists() && graphDataDir.isDirectory()) {
			
			FileOutputStream fileOutputStream = new FileOutputStream(graphZipFile);
			ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream);
			
			byte[] buffer = new byte[1024];
			
			for(File f : graphDataDir.listFiles()) {
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
			
			return graphZipFile;
					
		}
		
		return null;
		
	}
	
	private static boolean bucketCached(String graphId) throws IOException {
		File graphData = new File(GRAPH_DIR, graphId);
		
		// check if cached but only as zip
		if(!graphData.exists()) {
			File graphDataZip = new File(GRAPH_DIR, graphId + ".zip");
			
			if(graphDataZip.exists()) {
				zipGraphDir(graphData, graphDataZip);
			}
		}
		
		
		return graphData.exists() && graphData.isDirectory();
	}

	private void downloadGraphSourceFiles(String graphId, File dir) throws IOException {

		File graphCacheDir = dir;
		if (!graphCacheDir.exists())
			graphCacheDir.mkdirs();

		File graphZipFile = new File(graphCacheDir, graphId + ".zip");

		File extractedGraphDir = new File(graphCacheDir, graphId);

		if (extractedGraphDir.exists()) {
			FileUtils.deleteDirectory(extractedGraphDir);
		}

		extractedGraphDir.mkdirs();

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
	

	@Override
	public int evictAll() {
		graphMap.clear();
		return 0;
	}

	@Override
	public boolean evictGraph(String graphId) {
		graphMap.remove(graphId);
		return false;
	}

	@Override
	public Graph getGraph() {
		if(graphMap.values().size() > 0)
			return graphMap.values().iterator().next();
		return null;
	}

	@Override
	public Collection<String> getRouterIds() {
		return graphMap.keySet();
	}

	@Override
	public boolean registerGraph(String graphId, boolean arg1) {
		// TODO Auto-generated method stub
		return graphMap.get(graphId) != null;
	}

	@Override
	public boolean registerGraph(String arg0, Graph arg1) {
		graphMap.put(arg0,  arg1);
		return  true;
	}

	@Override
	public boolean reloadGraphs(boolean arg0) {
		return false;
	}

	@Override
	public boolean save(String arg0, InputStream arg1) {
		return false;
	}

	@Override
	public void setLoadLevel(LoadLevel arg0) {
	}
	
	
}
