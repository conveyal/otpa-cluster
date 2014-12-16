package com.conveyal.otpac.workers;

import java.util.Collection;

import akka.actor.ActorRef;

import com.conveyal.otpac.actors.Executive;

/**
 * Create and destroy workers for a particular environment.
 * 
 * For instance, we could have a LocalWorkerFactory, an EC2WorkerFactory, etc.
 */
public interface WorkerFactory {
	/**
	 * Create workers and return their IDs
	 * @param number the number of workers to create.
	 */
	public Collection<ActorRef> createWorkerManagers (int number, ActorRef executive);
	
	/**
	 * Terminate a worker.
	 */
	public void terminateWorkerManager (ActorRef actor);
}
