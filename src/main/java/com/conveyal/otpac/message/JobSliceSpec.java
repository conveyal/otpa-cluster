package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public String bucket;
	public Date date;
	public String fromPtsLoc;
	public Integer fromPtsStart = null;
	public Integer fromPtsEnd = null;
	public List<String> subsetIds = null;
	public String toPtsLoc;
	public String mode;
	
	
	public JobSliceSpec(String fromPtsLoc, int start, int end, String toPtsLoc, String bucket, Date date, String mode) {
		this.fromPtsLoc = fromPtsLoc;
		this.fromPtsStart = start;
		this.fromPtsEnd = end;
		this.toPtsLoc = toPtsLoc;
		this.bucket = bucket;
		this.date = date;
		this.mode = mode;
	}
	
	public JobSliceSpec(String fromPtsLoc, List<String> subsetIds, String toPtsLoc, String bucket, Date date, String mode) {
		this.fromPtsLoc = fromPtsLoc;
		this.subsetIds = subsetIds;
		this.toPtsLoc = toPtsLoc;
		this.bucket = bucket;
		this.date = date;
		this.mode = mode;
	}

	public String toString(){
		return "<JobSliceSpec from:"+fromPtsLoc+" to:"+toPtsLoc+" bucket:"+this.bucket+">";
	}

}
