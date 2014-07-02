package com.conveyal.otpac.standalone;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.Manager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class StandaloneCluster {
	ActorSystem system;
	
	public StandaloneCluster(){
		Config config = ConfigFactory.defaultOverrides();
		
		this.system = ActorSystem.create("MySystem", config);
	}

	public StandaloneExecutive createExecutive() {
		StandaloneExecutive ret = new StandaloneExecutive();
		
		ret.executive = system.actorOf(Props.create(Executive.class));
		
		return ret;
	}

	public StandaloneWorker createWorker() {
		StandaloneWorker ret = new StandaloneWorker();
		
		ret.manager = system.actorOf(Props.create(Manager.class), "manager");
		
		return ret;
	}

	public void registerWorker(StandaloneExecutive exec, StandaloneWorker worker) throws Exception {
		ActorSelection remoteManager = system.actorSelection(worker.getPath());
		
		exec.registerWorker(remoteManager);
	}
	
	
}