package com.conveyal.otpa.standalone;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;

public class StandaloneCluster {
	ActorSystem system;
	
	public StandaloneCluster(){
		Config config = ConfigFactory.defaultOverrides();
		
		this.system = ActorSystem.create("MySystem", config);
	}
}
