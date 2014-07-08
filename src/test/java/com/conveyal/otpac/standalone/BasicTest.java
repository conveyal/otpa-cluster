package com.conveyal.otpac.standalone;

import java.util.ArrayList;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.otpac.JobItemCallback;
import com.conveyal.otpac.standalone.StandaloneCluster;
import com.conveyal.otpac.standalone.StandaloneExecutive;
import com.conveyal.otpac.standalone.StandaloneWorker;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.WorkResult;

import junit.framework.TestCase;

public class BasicTest extends TestCase {
	public void testBasicSetup() throws Exception {
		StandaloneCluster cluster = new StandaloneCluster();

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		ArrayList<JobStatus> js = exec.getJobStatus();
		assertEquals(js.size(), 1);
		assertEquals(js.get(0).curJobId, -1);
	}

	public void testJob() throws Exception {
		// start up cluster
		StandaloneCluster cluster = new StandaloneCluster();

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		// build the request
		JobSpec js = new JobSpec("austin", "austin.csv", "austin.csv", "2014-06-09", "8:05 AM", "America/Chicago");

		// plus a callback that registers how many work items have returned
		class CounterCallback implements JobItemCallback {
			int jobsBack = 0;

			@Override
			public void onWorkResult(WorkResult res) {
				System.out.println("got callback");
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
	}
	
	public void testShapefile() throws Exception {
		// start up cluster
		StandaloneCluster cluster = new StandaloneCluster();

		StandaloneExecutive exec = cluster.createExecutive();
		StandaloneWorker worker = cluster.createWorker();

		cluster.registerWorker(exec, worker);

		// build the request
		JobSpec js = new JobSpec("austin", "austin.shp.zip", "austin.shp.zip", "2014-06-09", "8:05 AM", "America/Chicago");

		// plus a callback that registers how many work items have returned
		class CounterCallback implements JobItemCallback {
			int jobsBack = 0;

			@Override
			public void onWorkResult(WorkResult res) {
				System.out.println("got callback");
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
	}
}
