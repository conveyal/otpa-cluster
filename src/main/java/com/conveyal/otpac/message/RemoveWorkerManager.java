package com.conveyal.otpac.message;

import java.io.Serializable;

import akka.actor.ActorRef;

public class RemoveWorkerManager implements Serializable {

	public ActorRef workerManager;

	public RemoveWorkerManager(ActorRef workerManager) {
		this.workerManager = workerManager;
	}

}
