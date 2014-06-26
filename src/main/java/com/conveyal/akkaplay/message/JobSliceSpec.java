package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.Date;

import org.opentripplanner.analyst.PointSet;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public PointSet from;
	public String toPtsLoc;
	public String bucket;
	public Date date;

	public JobSliceSpec(PointSet from, String toPtsLoc, String bucket, Date date) {
		this.from = from;
		this.toPtsLoc = toPtsLoc;
		this.bucket = bucket;
		this.date = date;
	}
	
	public String toString(){
		return "<JobSliceSpec from.size:"+from.featureCount()+" to:"+toPtsLoc+" bucket:"+this.bucket+">";
	}

}
