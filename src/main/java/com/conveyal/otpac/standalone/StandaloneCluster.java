package com.conveyal.otpac.standalone;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class StandaloneCluster {
	ActorSystem system;
	
	public StandaloneCluster(String s3configfilename){
		Config config = ConfigFactory.parseString("s3.credentials.filename=\""+s3configfilename+"\"")
			    .withFallback(ConfigFactory.defaultOverrides());
		
		this.system = ActorSystem.create("MySystem", config);
	}

	public StandaloneExecutive createExecutive() {
		StandaloneExecutive ret = new StandaloneExecutive();
		
		ret.executive = system.actorOf(Props.create(Executive.class));
		
		return ret;
	}

	public StandaloneWorker createWorker(int nWorkers) {
		StandaloneWorker ret = new StandaloneWorker();
		
		ret.manager = system.actorOf(Props.create(WorkerManager.class, nWorkers), "manager");
		
		return ret;
	}
	
	public StandaloneWorker createWorker() {
		StandaloneWorker ret = new StandaloneWorker();
		
		ret.manager = system.actorOf(Props.create(WorkerManager.class), "manager");
		
		return ret;
	}

	public void registerWorker(StandaloneExecutive exec, StandaloneWorker worker) throws Exception {		
		exec.registerWorker(worker.getPath().toString());
	}

	public void stop(StandaloneWorker worker) {
		system.stop(worker.manager);
	}
	
	
}
