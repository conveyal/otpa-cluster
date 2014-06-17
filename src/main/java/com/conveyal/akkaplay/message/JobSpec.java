package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	public long start;
	public long end;
	public int jobId;

	public JobSpec(long start, long end) {
		this.start = start;
		this.end = end;
	}

}
