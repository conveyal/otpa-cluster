package com.conveyal.otpac.standalone;

import org.opentripplanner.routing.services.GraphService;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class StandaloneCluster {
	ActorSystem system;
	
	Boolean workOffline;
	
	private String pointsetsBucket;
	private String graphsBucket;
	
	public StandaloneCluster(Boolean workOffline, String pointsetsBucket, String graphsBucket){
		
		this.workOffline = workOffline;
		
		Config config = ConfigFactory.load();
		
		this.pointsetsBucket = pointsetsBucket;
		this.graphsBucket = graphsBucket;
		
		this.system = ActorSystem.create("MySystem", config);
	}

	public StandaloneExecutive createExecutive() {
		StandaloneExecutive ret = new StandaloneExecutive();
		
		ret.executive = system.actorOf(Props.create(Executive.class, workOffline, graphsBucket, pointsetsBucket));
		
		return ret;
	}

	public StandaloneWorker createWorker() {
		StandaloneWorker ret = new StandaloneWorker();
		
		ret.manager = system.actorOf(Props.create(WorkerManager.class, null, workOffline, graphsBucket, pointsetsBucket), "manager");
		
		return ret;
	}

	public void registerWorker(StandaloneExecutive exec, StandaloneWorker worker) throws Exception {		
		exec.registerWorker(worker.manager);
	}

	public void stop(StandaloneWorker worker) {
		system.stop(worker.manager);
	}
	
	
}
