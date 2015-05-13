package com.conveyal.otpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.PointSetCache;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.io.ByteStreams;

public class PointSetDatastore extends PointSetCache {

	static private File POINT_DIR = new File("cache", "pointsets");
	private String pointsetBucket;
	
	private AmazonS3Client s3;
	private final Boolean workOffline;
	
	public PointSetDatastore(Integer maxCacheSize, String s3CredentialsFilename,
			Boolean workOffline, String pointsetBucket){
		
		super();

		// allow the data store to work offline with cached data and skip S3 connection
		this.workOffline = workOffline;
		
		this.pointsetBucket = pointsetBucket;
		
		if(!this.workOffline) {
			if (s3CredentialsFilename != null) {
				AWSCredentials creds = new ProfileCredentialsProvider(s3CredentialsFilename, "default").getCredentials();
				s3 = new AmazonS3Client(creds);
			}
			else {
				// default credentials providers, e.g. IAM role
				s3 = new AmazonS3Client();
			}
		}
		
		// set up the cache
		this.pointSets = CacheBuilder.newBuilder()
			       .maximumSize(maxCacheSize)
			       .build(new S3PointSetLoader(workOffline, s3, pointsetBucket));
	}
	
	// adds file to S3 Data store or offline cache (if working offline)
	public String addPointSet(File pointSetFile, String pointSetId) throws IOException {
		
		if(pointSetId == null)
			pointSetId = Util.hashFile(pointSetFile) + "." + FilenameUtils.getExtension(pointSetFile.getName()).toLowerCase();
	
		File renamedPointSetFile = new File(POINT_DIR, pointSetId);
		
		if(renamedPointSetFile.exists())
			renamedPointSetFile.delete();
		
		FileUtils.copyFile(pointSetFile, renamedPointSetFile);
		
		if(!this.workOffline) {
			// only upload if it doesn't exist
			try {
				s3.getObjectMetadata(pointsetBucket, pointSetId + ".json.gz");
			} catch (AmazonServiceException e) {
				// gzip compression in storage, not because we're worried about file size but to speed file transfer
				FileInputStream fis = new FileInputStream(pointSetFile);
				File tempFile = File.createTempFile(pointSetId, ".json.gz");
				FileOutputStream fos = new FileOutputStream(tempFile);
				GZIPOutputStream gos = new GZIPOutputStream(fos);
				
				try {
					ByteStreams.copy(fis, gos);
				} finally {
					gos.close();
					fis.close();
				}
				
				s3.putObject(pointsetBucket, pointSetId + ".json.gz", tempFile);
				tempFile.delete();
			}
		} 
		
		return pointSetId;

	}
	
	/**
	 * Load pointsets from S3.
	 */
	protected static class S3PointSetLoader extends CacheLoader<String, PointSet> {

		private Boolean workOffline;
		private AmazonS3Client s3;
		private String pointsetBucket;
		
		/**
		 * Construct a new point set loader. S3 clients are generally threadsafe, so it's fine to share them.
		 */
		public S3PointSetLoader(Boolean workOffline, AmazonS3Client s3, String pointsetBucket) {
			this.workOffline = workOffline;
			this.s3 = s3;
			this.pointsetBucket = pointsetBucket;
		}

		@Override
		public PointSet load (String pointSetId) throws Exception {
			
			File cachedFile;
			
			if(!workOffline) {
				// get pointset metadata from S3
				cachedFile = new File(POINT_DIR, pointSetId + ".json");
				if(!cachedFile.exists()){
					POINT_DIR.mkdirs();
					
					S3Object obj = s3.getObject(pointsetBucket, pointSetId + ".json.gz");
					ObjectMetadata objMet = obj.getObjectMetadata();
					FileOutputStream fos = new FileOutputStream(cachedFile);
					GZIPInputStream gis = new GZIPInputStream(obj.getObjectContent());
					try {
						ByteStreams.copy(gis, fos);
					} finally {
						fos.close();
						gis.close();
					}
				}
			}
			else 
				cachedFile = new File(POINT_DIR, pointSetId + ".json");
			
			
			
			// grab it from the cache
			
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

	@Override
	public List<String> getPointSetIds() {
		// we have no clue what is in the S3 bucket.
		throw new UnsupportedOperationException("S3-backed point set datastore does not know what pointsets are available.");
	}
}
