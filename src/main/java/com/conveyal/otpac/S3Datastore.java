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

import org.apache.commons.io.IOUtils;
import org.opentripplanner.analyst.PointSet;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.Files;

public class S3Datastore {
	
	private AmazonS3Client s3;

	public S3Datastore(String filename){
		// grab credentials from "~.aws/credentials"
		AWSCredentials creds = new ProfileCredentialsProvider(filename, "default").getCredentials();
		s3 = new AmazonS3Client(creds);
	}

	public PointSet getPointset(String ptsLoc) throws Exception {

		// get pointset metadata from S3
		S3Object obj = s3.getObject("pointsets",ptsLoc);
		ObjectMetadata objMet = obj.getObjectMetadata();
		
		// if it's not already cached, do so
		String objEtag = objMet.getETag();
		File cachedFile = new File("cache/"+objEtag+"-"+ptsLoc);
		if(!cachedFile.exists()){
			Util.saveFile( cachedFile, obj.getObjectContent(), objMet.getContentLength(), true);
		}
		
		// grab it from the cache
		InputStream objectData = new FileInputStream( cachedFile );
		
		PointSet ret=null;
		if( isCsv(ptsLoc) ){
			ret = PointSet.fromCsv( "cache/"+objEtag+"-"+ptsLoc );
		} else if( isZippedShapefile(ptsLoc) ){
			File tempDir = Files.createTempDir();
			String shapefileName = unzipShapefile( tempDir, "cache/"+objEtag+"-"+ptsLoc );
			if(shapefileName == null){
				objectData.close();
				throw new Exception( "Zip does no contain a shapefile" );
			}
			
			ret = PointSet.fromShapefile( tempDir.getPath()+"/"+shapefileName);
			tempDir.delete();
		}
		
		objectData.close();
		return ret;

	}
	
	private boolean isZippedShapefile(String ptsLoc) {
		String[] parts = ptsLoc.split("\\.");
		return parts[parts.length-2].equals("shp") && parts[parts.length-1].equals("zip");
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
