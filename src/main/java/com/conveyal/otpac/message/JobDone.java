package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Set;

import akka.actor.ActorRef;

public class JobDone implements Serializable {

	private static final long serialVersionUID = 8212886436684219107L;
	public Set<ActorRef> workerManagers;
	public int jobId;
	
	public JobDone(int jobId, Set<ActorRef> managers) {
		this.jobId = jobId;
		this.workerManagers = managers;
	}


}
