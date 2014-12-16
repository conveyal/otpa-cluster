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
	GraphService graphService;
	
	public StandaloneCluster(String s3configfilename, Boolean workOffline, GraphService graphService){
		
		this.workOffline = workOffline;
		this.graphService = graphService;
		
		Config config = ConfigFactory.parseString("s3.credentials.filename=\""+s3configfilename+"\"")
			    .withFallback(ConfigFactory.defaultOverrides());
		
		this.system = ActorSystem.create("MySystem", config);
	}

	public StandaloneExecutive createExecutive() {
		StandaloneExecutive ret = new StandaloneExecutive();
		
		ret.executive = system.actorOf(Props.create(Executive.class, workOffline));
		
		return ret;
	}

	public StandaloneWorker createWorker() {
		StandaloneWorker ret = new StandaloneWorker();
		
		ret.manager = system.actorOf(Props.create(WorkerManager.class, null, workOffline, graphService), "manager");
		
		return ret;
	}

	public void registerWorker(StandaloneExecutive exec, StandaloneWorker worker) throws Exception {		
		exec.registerWorker(worker.manager);
	}

	public void stop(StandaloneWorker worker) {
		system.stop(worker.manager);
	}
	
	
}
