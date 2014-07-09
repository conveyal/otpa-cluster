package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;

import org.opentripplanner.analyst.PointSet;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public String bucket;
	public Date date;
	public String fromPtsLoc;
	public int fromPtsStart;
	public int fromPtsEnd;
	public String toPtsLoc;
	
	public JobSliceSpec(String fromPtsLoc, int start, int end, String toPtsLoc, String bucket, Date date) {
		this.fromPtsLoc = fromPtsLoc;
		this.fromPtsStart = start;
		this.fromPtsEnd = end;
		this.toPtsLoc = toPtsLoc;
		this.bucket = bucket;
		this.date = date;
	}

	public String toString(){
		return "<JobSliceSpec from:"+fromPtsLoc+" to:"+toPtsLoc+" bucket:"+this.bucket+">";
	}

}
