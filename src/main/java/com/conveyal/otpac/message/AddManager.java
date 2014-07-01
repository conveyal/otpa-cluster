package com.conveyal.otpac.message;

import akka.actor.ActorSelection;

public class AddManager {
	public ActorSelection remote;

	public AddManager(ActorSelection remote) {
		this.remote = remote;
	}

}
