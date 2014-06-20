package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	
	public String bucket;
	public int jobId;
	public String fromPtsLoc;
	public String toPtsLoc;

	public JobSpec(String bucket, String fromPtsLoc, String toPtsLoc) {
		this.bucket = bucket;
		this.fromPtsLoc = fromPtsLoc;
		this.toPtsLoc = toPtsLoc;
	}

}
