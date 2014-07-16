package com.conveyal.otpac.standalone;

import java.util.ArrayList;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.otpac.message.AddWorkerManager;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;

public class StandaloneExecutive {
	
	ActorRef executive;
	
	protected void registerWorker(String path) throws Exception {				
		// Block until success. We don't need the result; we're just preventing race conditions.
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(executive, new AddWorkerManager(path), timeout);
		Await.result(future, timeout.duration());
	}

	@SuppressWarnings("unchecked")
	public ArrayList<JobStatus> getJobStatus() throws Exception {
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(executive, new JobStatusQuery(), timeout);
		ArrayList<JobStatus> result = (ArrayList<JobStatus>) Await.result(future, timeout.duration());
		
		return result;
	}

	public JobId find(JobSpec js) throws Exception {
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(executive, js, timeout);
		JobId result = (JobId) Await.result(future, timeout.duration());
		
		return result;
	}

}
