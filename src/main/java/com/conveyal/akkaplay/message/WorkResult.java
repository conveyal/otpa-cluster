package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.conveyal.akkaplay.Histogram;
import com.conveyal.akkaplay.Point;

public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public List<Histogram> histograms;
	public Point point=null;

	public WorkResult(boolean success) {
		this.success = success;
		this.histograms = new ArrayList<Histogram>();
	}
	
	public String toString(){
		return "<Job success:"+success+" point:"+point+" histograms.size:"+histograms.size()+">";
	}

	public void addHistogram(String key, List<Float> value) {
		this.histograms.add( new Histogram(key,value) );
	}

}
