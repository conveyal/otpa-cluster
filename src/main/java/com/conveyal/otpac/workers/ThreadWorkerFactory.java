package com.conveyal.otpac.workers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.conveyal.otpac.message.AddWorkerManager;
import com.conveyal.otpac.message.RemoveWorkerManager;
import com.conveyal.otpac.standalone.StandaloneWorker;

/**
 * Create workers in local threads.
 * @author mattwigway
 *
 */
public class ThreadWorkerFactory implements WorkerFactory {
	public final ActorSystem system;
	
	public final String pointsetsBucket, graphsBucket;
	public final Boolean workOffline;
	public final Integer nWorkers;
	
	public ThreadWorkerFactory(ActorSystem system, Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		this(system, workOffline, graphsBucket, pointsetsBucket, null);
	}
	
	/**
	 * Create a new thread worker factory
	 * @param system Actor system in which to create the worker
	 * @param workOffline Should the actors run offline (i.e. no S3)
	 * @param graphsBucket S3 bucket where graphs are stored
	 * @param pointsetsBucket S3 bucket where pointsets are stored 
	 * @param nWorkers number of threads to create (null for number of available processors)
	 */
	public ThreadWorkerFactory(ActorSystem system, Boolean workOffline, String graphsBucket, String pointsetsBucket, Integer nWorkers) {
		this.system = system;
		this.graphsBucket = graphsBucket;
		this.pointsetsBucket = pointsetsBucket;
		this.workOffline = workOffline;
		this.nWorkers = nWorkers;
	}
	
	public Collection<ActorRef> createWorkerManagers(int number, ActorRef executive) {
		List<ActorRef> ret = new ArrayList<ActorRef>();
		
		for (int i = 0; i < number; i++) {
			// we use a UUID to identify the actor so that if the actor respawns it will not have the same path.
			// see issue 
			ActorRef manager = system.actorOf(Props.create(WorkerManager.class, executive, nWorkers, workOffline, graphsBucket, pointsetsBucket), "manager_" + 
					UUID.randomUUID().toString());
			ret.add(manager);
		}
				
		return ret;
	}

	public void terminateWorkerManager(ActorRef actor, ActorRef executive) {
		executive.tell(new RemoveWorkerManager(actor), ActorRef.noSender());
		system.stop(actor);
	}

}
