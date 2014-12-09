package com.conveyal.otpac.standalone;

import java.io.IOException;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.opentripplanner.analyst.Histogram;
import org.opentripplanner.profile.ProfileRequest;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.core.TraverseModeSet;

import com.conveyal.otpac.JobItemCallback;
import com.conveyal.otpac.PrototypeAnalystProfileRequest;
import com.conveyal.otpac.PrototypeAnalystRequest;
import com.conveyal.otpac.standalone.StandaloneCluster;
import com.conveyal.otpac.standalone.StandaloneExecutive;
import com.conveyal.otpac.standalone.StandaloneWorker;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.WorkResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.TestCase;

public class BasicTest extends TestCase {
	public void testBasicSetup() throws Exception {
		StandaloneCluster cluster = new StandaloneCluster("s3credentials", true, null);

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		ArrayList<JobStatus> js = exec.getJobStatus();
		assertEquals(js.size(), 1);
		assertEquals(js.get(0).curJobId, -1);
	}

	public void testJob() throws Exception {
		// start up cluster
		StandaloneCluster cluster = new StandaloneCluster("s3credentials", true, null);

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		// build the request
		RoutingRequest rr = new PrototypeAnalystRequest();		
		DateTime t = new DateTime(2014, 6, 9, 8, 5, DateTimeZone.forID("America/Chicago"));
		rr.dateTime = t.getMillis() / 1000;
		rr.setModes(new TraverseModeSet("TRANSIT"));
		
		JobSpec js = new JobSpec("austin", "austin.csv", "austin.csv", rr);

		// plus a callback that registers how many work items have returned
		class CounterCallback implements JobItemCallback {
			int jobsBack = 0;

			@Override
			public void onWorkResult(WorkResult res) {
				System.out.println("got callback");
				jobsBack += 1;
			}
		};
		
		CounterCallback callback = new CounterCallback();
		js.setCallback(callback);

		// start the job
		exec.find(js);

		// stall until a work item returns
		while (callback.jobsBack == 0) {
			Thread.sleep(100);
		}
		
		cluster.stop(worker);
	}
	
	public void testShapefile() throws Exception {
		// start up cluster
		StandaloneCluster cluster = new StandaloneCluster("s3credentials", true, null);

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		// build the request
		RoutingRequest rr = new PrototypeAnalystRequest();		
		DateTime t = new DateTime(2014, 6, 9, 8, 5, DateTimeZone.forID("America/Chicago"));
		rr.dateTime = t.getMillis() / 1000;
		rr.setModes(new TraverseModeSet("TRANSIT"));
		
		JobSpec js = new JobSpec("austin", "austin.shp.zip", "austin.shp.zip", rr);

		// plus a callback that registers how many work items have returned
		class CounterCallback implements JobItemCallback {
			int jobsBack = 0;
			String jsonBack = null;

			@Override
			public synchronized void onWorkResult(WorkResult res) {
				try {
					jsonBack =  res.toJsonString();
				} catch (IOException e) {
					jsonBack = null;
				}
				
				assertNotNull(jsonBack);
				if(jsonBack!=null){
					ObjectMapper mapper = new ObjectMapper();
					try {
						Object back = mapper.readValue(jsonBack.getBytes(), Object.class);
						assertNotNull(back);
					} catch (IOException e) {
						e.printStackTrace();
						assertTrue(false);
					}
					
					assertEquals(jsonBack.substring(0,10), "{\"jobId\":0");
				}

				jobsBack += 1;
			}
		}
		;
		CounterCallback callback = new CounterCallback();
		js.setCallback(callback);

		// start the job
		exec.find(js);

		// stall until a work item returns
		while (callback.jobsBack == 0) {
			Thread.sleep(100);
		}
		
		cluster.stop(worker);
	}
	
	public void testProfile() throws Exception {
		// start up cluster
		StandaloneCluster cluster = new StandaloneCluster("s3credentials", true, null);

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		// build the request
		ProfileRequest opts = new PrototypeAnalystProfileRequest();		
		// 7:00 AM, ignoring any leap seconds
		opts.fromTime = 7 * 3600;
		// 9 AM
		opts.toTime = 9 * 3600;
		opts.date = new LocalDate(2014, 6, 9);
		
		opts.accessModes = opts.egressModes = opts.directModes = new TraverseModeSet("WALK");
		opts.transitModes = new TraverseModeSet("TRANSIT");
		
		
		JobSpec js = new JobSpec("austin", "austin.csv", "austin.csv", opts);

		// plus a callback that registers how many work items have returned
		class CounterCallback implements JobItemCallback {
			public int cumulativeOpportunities = 0;
			public int jobsBack = 0;

			@Override
			public void onWorkResult(WorkResult res) {
				System.out.println("got callback");
				jobsBack += 1;
				
				assertTrue(res.success);
				assertTrue(res.profile);
				
				// integrate the opportunities from 0 to 60 minutes
				Histogram rf = res.getMinimum().histograms.values().iterator().next();
				for (int i = 0; i < rf.counts.length; i++) {
					cumulativeOpportunities += rf.counts[i];
				}
			}
		};
		
		CounterCallback callback = new CounterCallback();
		js.setCallback(callback);

		// start the job
		exec.find(js);

		// stall until a few work items return
		// we wait for a few because the first may be in an unreachable location
		// and thus there are no opportunities reachable from it.
		while (callback.jobsBack < 5) {
			Thread.sleep(100);
		}
		assertTrue(callback.cumulativeOpportunities > 0);
		
		cluster.stop(worker);
	}
}
