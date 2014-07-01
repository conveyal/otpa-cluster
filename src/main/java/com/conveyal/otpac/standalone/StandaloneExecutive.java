package com.conveyal.otpac.standalone;

import java.util.ArrayList;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.message.AddManager;
import com.conveyal.otpac.message.JobResult;
import com.conveyal.otpac.message.JobResultQuery;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;

public class StandaloneExecutive {
	
	StandaloneCluster cluster;
	ActorRef executive;
	
	public StandaloneExecutive(StandaloneCluster cluster){
		this.cluster = cluster;
		executive = cluster.system.actorOf(Props.create(Executive.class));
	}

	public void registerWorker(StandaloneWorker worker) throws Exception {
		ActorSelection remoteManager = cluster.system.actorSelection(worker.getPath());
		
		executive.tell(new AddManager(remoteManager), ActorRef.noSender());
		
		// Block until success. We don't need the result; we're just preventing race conditions.
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(executive, new AddManager(remoteManager), timeout);
		Boolean result = (Boolean) Await.result(future, timeout.duration());
	}

	public ArrayList<JobStatus> getJobStatus() throws Exception {
		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		Future<Object> future = Patterns.ask(executive, new JobStatusQuery(), timeout);
		ArrayList<JobStatus> result = (ArrayList<JobStatus>) Await.result(future, timeout.duration());
		
		return result;
	}

}
