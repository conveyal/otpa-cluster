package com.conveyal.otpac.message;

import akka.actor.ActorRef;

/**
 * Assign a worker manager to a job manager.
 * 
 * @author mattwigway
 */
public class AssignWorkerManager {
	public final ActorRef workerManager;
	
	public AssignWorkerManager (ActorRef workerManager) {
		this.workerManager = workerManager; 
	}
}
