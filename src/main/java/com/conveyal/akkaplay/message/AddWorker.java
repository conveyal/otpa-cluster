package com.conveyal.akkaplay.message;

import akka.actor.ActorSelection;

public class AddWorker {
	public ActorSelection path;

	public AddWorker(ActorSelection remoteTaskMaster) {
		this.path = remoteTaskMaster;
	}

}
