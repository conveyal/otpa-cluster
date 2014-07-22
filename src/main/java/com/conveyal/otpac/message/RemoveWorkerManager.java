package com.conveyal.otpac.message;

import akka.actor.ActorRef;

public class RemoveWorkerManager {

	public ActorRef workerManager;

	public RemoveWorkerManager(ActorRef workerManager) {
		this.workerManager = workerManager;
	}

}
