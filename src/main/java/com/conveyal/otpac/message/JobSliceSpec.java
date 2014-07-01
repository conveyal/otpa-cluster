package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;

import org.opentripplanner.analyst.PointSet;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public PointSet from;
	public PointSet to;
	public String bucket;
	public Date date;

	public JobSliceSpec(PointSet from, PointSet to, String bucket, Date date) {
		this.from = from;
		this.to = to;
		this.bucket = bucket;
		this.date = date;
	}
	
	public String toString(){
		return "<JobSliceSpec from.size:"+from.featureCount()+" to.size:"+to.featureCount()+" bucket:"+this.bucket+">";
	}

}
