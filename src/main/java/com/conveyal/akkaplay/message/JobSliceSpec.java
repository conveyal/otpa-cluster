package com.conveyal.akkaplay.message;

import java.io.Serializable;

import com.conveyal.akkaplay.Pointset;

public class JobSliceSpec implements Serializable {

	private static final long serialVersionUID = -2994498856080149222L;
	
	public Pointset from;
	public Pointset to;
	public String bucket;

	public JobSliceSpec(Pointset from, Pointset to, String bucket) {
		this.from = from;
		this.to = to;
		this.bucket = bucket;
	}
	
	public String toString(){
		return "<JobSliceSpec from.size:"+from.size()+" to.size:"+to.size()+" bucket:"+this.bucket+">";
	}

}
