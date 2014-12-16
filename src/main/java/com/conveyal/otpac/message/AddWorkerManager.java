package com.conveyal.otpac.message;

import java.io.Serializable;

import akka.actor.ActorRef;

public class AddWorkerManager implements Serializable {
	public ActorRef workerManager;

	public AddWorkerManager(ActorRef workerManager) {
		this.workerManager = workerManager;
	}

}
