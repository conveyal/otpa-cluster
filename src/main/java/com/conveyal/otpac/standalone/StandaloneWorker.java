package com.conveyal.otpac.standalone;

import akka.actor.ActorPath;
import akka.actor.ActorRef;

public class StandaloneWorker {

	ActorRef manager;

	public ActorPath getPath() {
		return this.manager.path();
	}

}
