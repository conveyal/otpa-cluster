package com.conveyal.otpa.standalone;

import java.util.ArrayList;

import com.conveyal.otpa.standalone.StandaloneCluster;
import com.conveyal.otpa.standalone.StandaloneExecutive;
import com.conveyal.otpa.standalone.StandaloneWorker;
import com.conveyal.otpac.message.JobStatus;

import junit.framework.TestCase;

public class BasicTest extends TestCase {
    public void testBasicSetup() throws Exception {
    	StandaloneCluster cluster = new StandaloneCluster();
    	
    	StandaloneExecutive exec = new StandaloneExecutive(cluster);
    	StandaloneWorker worker = new StandaloneWorker(cluster);
    	 
    	exec.registerWorker(worker);
    	
    	ArrayList<JobStatus> js = exec.getJobStatus();
    	assertEquals( js.size(),  1 );
    	assertEquals( js.get(0).curJobId, -1 );    	
    }
}
