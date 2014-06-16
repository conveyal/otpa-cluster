package com.conveyal.akkaplay;

import akka.actor.ActorSelection;

public class AddWorker {
	ActorSelection path;

	public AddWorker(ActorSelection remoteTaskMaster) {
		this.path = remoteTaskMaster;
	}

}
