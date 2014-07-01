package com.conveyal.otpac.standalone;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;

import com.conveyal.otpac.actors.Manager;

public class StandaloneWorker {

	StandaloneCluster cluster;
	ActorRef manager;

	public StandaloneWorker(StandaloneCluster cluster) {
		this.cluster = cluster;
		this.manager = cluster.system.actorOf(Props.create(Manager.class), "manager");
	}

	public ActorPath getPath() {
		return this.manager.path();
	}

}
