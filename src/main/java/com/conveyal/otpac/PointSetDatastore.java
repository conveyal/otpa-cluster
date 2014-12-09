package com.conveyal.otpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.analyst.DiskBackedPointSetCache;
import org.opentripplanner.analyst.PointSet;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.Files;

public class PointSetDatastore extends DiskBackedPointSetCache {

	static private File POINT_DIR = new File("cache", "pointsets");
	static private String pointsetBucket = "otpac-pointsets";
	
	private AmazonS3Client s3;
	private Boolean workOffline = false;
	
	public PointSetDatastore(String s3CredentialsFilename) {
		
		// don't work offline by default
		this(10, s3CredentialsFilename, false);
	}
	
	public PointSetDatastore(Integer maxCacheSize, String s3CredentialsFilename, Boolean workOffline){
		super(maxCacheSize,POINT_DIR);

		// allow the data store to work offline with cached data and skip S3 connection
		this.workOffline = workOffline;
		
		if(!this.workOffline) {
			// grab credentials from "~.aws/credentials"
			AWSCredentials creds = new ProfileCredentialsProvider(s3CredentialsFilename, "default").getCredentials();
			s3 = new AmazonS3Client(creds);
		}
	}
	
	// adds file to S3 Data store or offline cache (if working offline)
	public String addPointSet(File pointSetFile, String pointSetId) throws IOException {
		
		if(pointSetId == null)
			pointSetId = Util.hashFile(pointSetFile) + "." + FilenameUtils.getExtension(pointSetFile.getName()).toLowerCase();
	
		File renamedPointSetFile = new File(POINT_DIR, pointSetId);
		
		FileUtils.copyFile(pointSetFile, renamedPointSetFile);
		
		if(!this.workOffline) {
			PutObjectResult obj = s3.putObject(pointsetBucket, pointSetId, pointSetFile);	
		} 
		else {
			if(renamedPointSetFile.exists())
				renamedPointSetFile.delete();
			
			FileUtils.copyFile(pointSetFile, renamedPointSetFile);
		}
		
		return pointSetId;

	}

	public PointSet getPointset(String pointSetId) throws Exception {
		
		File cachedFile;
		
		if(!this.workOffline) {
			// get pointset metadata from S3
			S3Object obj = s3.getObject(pointsetBucket, pointSetId);
			ObjectMetadata objMet = obj.getObjectMetadata();
			cachedFile = new File(POINT_DIR, pointSetId);
			if(!cachedFile.exists()){
				Util.saveFile( cachedFile, obj.getObjectContent(), objMet.getContentLength(), true);
			}
		}
		else 
			cachedFile = new File(POINT_DIR, pointSetId);
		
		
		
		// grab it from the cache
		InputStream objectData = new FileInputStream(cachedFile);
		
		PointSet ret=null;
		if( isCsv(cachedFile) ){
			ret = PointSet.fromCsv(cachedFile);
		} else if(isJson(cachedFile)) {
			ret = PointSet.fromGeoJson(cachedFile);
		}
		else if(isShp(cachedFile)) {
			ret = PointSet.fromShapefile(cachedFile);
		}

		else if(isZip(cachedFile)) {
			
			File tmpDir = new File(POINT_DIR, UUID.randomUUID().toString());
			tmpDir.mkdirs();
			
			unzipDirectory(tmpDir, cachedFile);
			
			for(File f : tmpDir.listFiles()) {
				if(isShp(f))
					ret = PointSet.fromShapefile(f);
			}
				
		}
		
		objectData.close();
		return ret;

	}
	
	private boolean isZip(File f) {
		return f.getName().toLowerCase().endsWith(".zip");
	}
	
	private boolean isShp(File f) {
		return f.getName().toLowerCase().endsWith(".shp");
	}

	private boolean isJson(File f) {
		return f.getName().toLowerCase().endsWith(".json");
	}
	
	private boolean isCsv(File f) {
		return f.getName().toLowerCase().endsWith(".csv");
	}
	
	/**
	 * 
	 * @param Directory to unzip into.
	 * @param File to unzip.
	 * @return The name of the shapefile.
	 * @throws IOException
	 */
	private String unzipDirectory(File dir, File file) throws IOException {
		String retval=null;
		
		ZipFile zipFile = new ZipFile(file);
	    Enumeration<? extends ZipEntry> entries = zipFile.entries();
	    while (entries.hasMoreElements()) {
	        ZipEntry entry = entries.nextElement();
	        
	        String name = entry.getName().toLowerCase();
	        if(name.endsWith(".shp")){
	        	retval = name;
	        }
	        
	        File entryDestination = new File(dir, name);
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
