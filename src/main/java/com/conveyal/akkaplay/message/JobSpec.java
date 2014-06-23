package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	
	public String bucket;
	public int jobId;
	public String fromPtsLoc;
	public String toPtsLoc;

	public String date;
	public String time;
	public String tz;

	public JobSpec(String bucket, String fromPtsLoc, String toPtsLoc, String date, String time, String tz) {
		this.bucket = bucket;
		this.fromPtsLoc = fromPtsLoc;
		this.toPtsLoc = toPtsLoc;
		this.date = date;
		this.time = time;
		this.tz = tz;
	}

}
