package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public String bucket;
	
	/** JobSpec for this slice */
	public JobSpec jobSpec;
	
	/** The start of the slice of this pointset that this worker should compute */
	public Integer fromPtsStart = null;
	
	/** The end of the slice */
	public Integer fromPtsEnd = null;

	public JobSliceSpec(JobSpec js, int start, int end, String bucket) {
		this.fromPtsStart = start;
		this.fromPtsEnd = end;
		this.jobSpec = js;
		this.bucket = bucket;
	}
	
	public String toString(){
		return "<JobSliceSpec from:" + jobSpec.fromPtsLoc + " to:" + jobSpec.toPtsLoc + " bucket:" + this.bucket + ">";
	}

}
