package com.conveyal.akkaplay;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.PointSetCache;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.services.GraphService;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

public class S3BackedPointSetCache extends PointSetCache {
	
	private static final Logger LOG = LoggerFactory.getLogger(S3BackedPointSetCache.class);
	protected File pointSetPath;
	
	AmazonS3 s3;
	
	protected String s3Bucket;
	
	protected class S3PointSetLoader extends CacheLoader<String, PointSet> {
	
		@Override
		public PointSet load(String pointSetId) throws Exception {
			if (pointSetPath != null && ! (pointSetPath.isDirectory() && pointSetPath.canRead())) {
                LOG.error("'{}' is not a readable directory.", pointSetPath);
                return null;
            }
			
    		for (File file : pointSetPath.listFiles()) {
               
                if(file.getName().toLowerCase().startsWith(pointSetId.toLowerCase())) {
                	
                	PointSet pointSet = this.loadFromFile(file);
                	
                	if(pointSet != null)
                		return pointSet;
                	
                }
            }
    		
    		// if not in file system grab from s3 -- TODO assumes only json pointset currently (may want to stay that way!)
    		
    		S3Object obj = s3.getObject(s3Bucket,pointSetId + ".json");
    		
    		ObjectMetadata objMet = obj.getObjectMetadata();
    		
    		File cachedFile = new File(pointSetPath, pointSetId + ".json");
    		Util.saveFile( cachedFile, obj.getObjectContent(), objMet.getContentLength(), true);
    		
			return this.loadFromFile(cachedFile);
		}
		
		public PointSet loadFromFile(File pointSetData) {
			
			String name = pointSetData.getName();
			
			if (name.endsWith(".csv")) {
	            String baseName = name.substring(0, name.length() - 4);
	            LOG.info("loading '{}' with ID '{}'", pointSetData, baseName);
	            try {
	                PointSet pset = PointSet.fromCsv(pointSetData.getAbsolutePath());
	                if (pset == null) {
	                    LOG.warn("Failure, skipping this pointset.");
	                }
	                
	                pset.setGraphService(graphService);

	                return pset;
	                
	            } catch (IOException ioex) {
	                LOG.warn("Exception while loading pointset: {}", ioex);
	            }
	            
	        } else if (name.endsWith(".json")) {
	            String baseName = name.substring(0, name.length() - 5);
	            LOG.info("loading '{}' with ID '{}'", pointSetData, baseName);
	            PointSet pset = PointSet.fromGeoJson(pointSetData.getAbsolutePath());
	            if (pset == null) {
	                LOG.warn("Failure, skipping this pointset.");
	            }
	            
	            pset.setGraphService(graphService);
	            
	            return pset;
	        }
			
			return null;
		}
		
	}
	
	public S3BackedPointSetCache(Integer maxCacheSize, String s3Bucket, File pointSetPath, GraphService graphService) {
		super(graphService);
		this.s3Bucket = s3Bucket;
		this.pointSetPath = pointSetPath;
		
		AWSCredentials creds = new ProfileCredentialsProvider().getCredentials();
		s3 = new AmazonS3Client(creds);
		
		// TODO could convert to a weight-based eviction strategy based on pointset size
		this.pointSets = CacheBuilder.newBuilder()
			       .maximumSize(maxCacheSize)
			       .build(new S3PointSetLoader());
		
	}

	@Override
	public ArrayList<String> getPointSetIds() {
		
		ArrayList<String> ids = new ArrayList<String>();
		
		for(File f : pointSetPath.listFiles()) {
			String name = f.getName();
			
			if (name.endsWith(".csv")) {
	            String baseName = name.substring(0, name.length() - 4);
	            ids.add(baseName);
			}
			else if (name.endsWith(".json")) {
		        String baseName = name.substring(0, name.length() - 5);
		        ids.add(baseName);
			}
			
		}
		
		return ids;
	}
	
}
