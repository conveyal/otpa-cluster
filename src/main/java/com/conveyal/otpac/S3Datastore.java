package com.conveyal.otpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.analyst.PointSet;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.Files;

public class S3Datastore {
	
	private static String CACHE_DIR = "cache/";
	
	private AmazonS3Client s3;
	private Boolean workOffline = false;
	
	public S3Datastore() {
		// don't work offline by default
		this(false);
	}
	
	public S3Datastore(Boolean workOffline){
		
		// allow the data store to work offline with cached data and skip S3 connection
		this.workOffline = workOffline;
		
		if(!this.workOffline) {
			// grab credentials from "~.aws/credentials"
			AWSCredentials creds = new ProfileCredentialsProvider().getCredentials();
			s3 = new AmazonS3Client(creds);
		}
	}
	
	
	// adds file to S3 Data store or offline cache (if working offline)
	public void addPointSet(File pointSetFile) throws IOException {
		
		String ptsLoc = pointSetFile.getName();
		
		if(!this.workOffline) {
			PutObjectResult obj = s3.putObject("pointsets",ptsLoc, pointSetFile);	
		} 
		else {
			FileUtils.moveFileToDirectory(pointSetFile, new File(CACHE_DIR), true);
		}
	}

	public PointSet getPointset(String ptsLoc) throws Exception {

		String cachePath;
		
		if(!this.workOffline) {
			// get pointset metadata from S3
			S3Object obj = s3.getObject("pointsets",ptsLoc);
			ObjectMetadata objMet = obj.getObjectMetadata();
			
			// if it's not already cached, do so
			String objEtag = objMet.getETag();
			
			cachePath = CACHE_DIR + objEtag+"-"+ptsLoc;
			
			File cachedFile = new File(cachePath);
			if(!cachedFile.exists()){
				Util.saveFile( cachedFile, obj.getObjectContent(), objMet.getContentLength(), true);
			}
		}
		else 
			cachePath = CACHE_DIR + ptsLoc;
		
		
		
		// grab it from the cache
		InputStream objectData = new FileInputStream(cachePath);
		
		PointSet ret=null;
		if( isCsv(ptsLoc) ){
			ret = PointSet.fromCsv(cachePath);
		} else if( isZippedShapefile(ptsLoc) ){
			File tempDir = Files.createTempDir();
			String shapefileName = unzipShapefile( tempDir,cachePath);
			if(shapefileName == null){
				objectData.close();
				throw new Exception( "Zip does no contain a shapefile" );
			}
			
			ret = PointSet.fromShapefile( tempDir.getPath()+"/"+shapefileName);
			tempDir.delete();
		} else if(isJson(ptsLoc)) {
			ret = PointSet.fromGeoJson(cachePath);
		}
		
		objectData.close();
		return ret;

	}
	
	private boolean isZippedShapefile(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		return parts[parts.length-2].equals("shp") && parts[parts.length-1].equals("zip");
	}

	private boolean isJson(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		String formatPart = parts[parts.length-1];
		return formatPart.equals("json");
	}
	
	private boolean isCsv(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		String formatPart = parts[parts.length-1];
		return formatPart.equals("csv");
	}
	
	/**
	 * 
	 * @param Directory to unzip into.
	 * @param File to unzip.
	 * @return The name of the shapefile.
	 * @throws IOException
	 */
	private String unzipShapefile(File dir, String file) throws IOException {
		String retval=null;
		
		ZipFile zipFile = new ZipFile(file);
	    Enumeration<? extends ZipEntry> entries = zipFile.entries();
	    while (entries.hasMoreElements()) {
	        ZipEntry entry = entries.nextElement();
	        
	        String name = entry.getName();
	        if(name.endsWith(".shp")){
	        	retval = name;
	        }
	        
	        File entryDestination = new File(dir,  name);
	        entryDestination.getParentFile().mkdirs();
	        if (entry.isDirectory())
	            entryDestination.mkdirs();
	        else {
	            InputStream in = zipFile.getInputStream(entry);
	            OutputStream out = new FileOutputStream(entryDestination);
	            IOUtils.copy(in, out);
	            IOUtils.closeQuietly(in);
	            IOUtils.closeQuietly(out);
	        }
	    }
	    zipFile.close();
	    
	    return retval;
	}

}
