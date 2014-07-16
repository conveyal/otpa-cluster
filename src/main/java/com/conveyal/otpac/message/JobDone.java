package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.ArrayList;

import akka.actor.ActorRef;

public class JobDone implements Serializable {

	private static final long serialVersionUID = 8212886436684219107L;
	public ArrayList<ActorRef> managers;
	public int jobId;
	
	public JobDone(int jobId, ArrayList<ActorRef> managers) {
		this.jobId = jobId;
		this.managers = managers;
	}


}
