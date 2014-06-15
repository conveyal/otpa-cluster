package com.conveyal.akkaplay;

import java.io.Serializable;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	long start;
	long end;

	public JobSpec(long start, long end) {
		this.start = start;
		this.end = end;
	}

}