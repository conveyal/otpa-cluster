package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Set;

import com.conveyal.otpac.message.JobDone.Status;

import akka.actor.ActorRef;

public class JobDone implements Serializable {

	private static final long serialVersionUID = 8212886436684219107L;
	public static enum Status { SUCCESS, FAILURE, CANCELLED };
	public Set<ActorRef> workerManagers;
	public int jobId;
	public Status status;
	
	public JobDone(int jobId, Set<ActorRef> managers) {
		this.jobId = jobId;
		this.workerManagers = managers;
		this.status = Status.SUCCESS;
	}

	public JobDone(Status status, Set<ActorRef> workerManagers) {
		this.status = status;
		this.workerManagers = workerManagers;
	}


}
