package com.conveyal.otpac.message;

import java.io.Serializable;

import akka.actor.ActorRef;

public class JobStatus implements Serializable{

	private static final long serialVersionUID = 1941471771774801942L;
	public ActorRef manager=null;
	public int curJobId;
	public long total;
	public long complete;
	public float fractionComplete;

	
	public JobStatus(int curJobId, long total, long complete) {
		this.curJobId = curJobId;
		this.total = total;
		this.complete = complete;
		this.fractionComplete = (float)complete / (float)total;
	}

	public JobStatus(ActorRef manager, int curJobId, long total, long complete) {
		this.manager = manager;
		this.curJobId = curJobId;
		this.total = total;
		this.complete = complete;
		this.fractionComplete = (float)complete / (float)total;
	}
	
	public Boolean isComplete() {
		if(total >= 0 && complete == total)
			return true;
		else
			return false;
	}

}
