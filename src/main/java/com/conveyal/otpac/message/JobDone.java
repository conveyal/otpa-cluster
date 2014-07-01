package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.ArrayList;

import akka.actor.ActorSelection;

public class JobDone implements Serializable {

	private static final long serialVersionUID = 8212886436684219107L;
	public ArrayList<ActorSelection> managers;
	public int jobId;
	
	public JobDone(int jobId, ArrayList<ActorSelection> managers) {
		this.jobId = jobId;
		this.managers = managers;
	}


}
