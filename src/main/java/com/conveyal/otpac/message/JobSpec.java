package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.ArrayList;

import com.conveyal.otpac.standalone.JobItemCallback;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	
	public String graphId;
	public int jobId;
	public String fromPtsLoc;
	public String toPtsLoc;

	public String date;
	public String time;
	public String tz;

	public JobItemCallback callback=null;

	public JobSpec(String graphId, String fromPtsLoc, String toPtsLoc, String date, String time, String tz) {
		this.graphId = graphId;
		this.fromPtsLoc = fromPtsLoc;
		this.toPtsLoc = toPtsLoc;
		this.date = date;
		this.time = time;
		this.tz = tz;
	}

	public void setCallback(JobItemCallback callback) {
		this.callback = callback;
	}

}
