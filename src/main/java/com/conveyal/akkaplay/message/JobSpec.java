package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	public String bucket;
	public int jobId;

	public JobSpec(String bucket) {
		this.bucket = bucket;
	}

}
