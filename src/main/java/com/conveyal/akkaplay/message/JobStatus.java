package com.conveyal.akkaplay.message;

import java.io.Serializable;

import akka.actor.ActorRef;

public class JobStatus implements Serializable{

	private static final long serialVersionUID = 1941471771774801942L;
	public ActorRef manager=null;
	public int curJobId;
	public float fractionComplete;

	public JobStatus(int curJobId, float fractionComplete) {
		this.curJobId = curJobId;
		this.fractionComplete = fractionComplete;
	}

	public JobStatus(ActorRef manager, int curJobId, float fractionComplete) {
		this.manager = manager;
		this.curJobId = curJobId;
		this.fractionComplete = fractionComplete;
	}

}
