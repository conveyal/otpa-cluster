package com.conveyal.otpac.standalone;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;

import com.conveyal.otpac.actors.Manager;

public class StandaloneWorker {

	ActorRef manager;

	public ActorPath getPath() {
		return this.manager.path();
	}

}
