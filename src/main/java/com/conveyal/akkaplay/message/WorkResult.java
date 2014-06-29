package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.geotools.filter.expression.ThisPropertyAccessorFactory;
import org.json.JSONObject;
import org.opentripplanner.analyst.PointFeature;

import com.conveyal.akkaplay.Histogram;

public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public List<Histogram> histograms;
	public PointFeature point=null;
	public int jobId;

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

	public String toJsonString() {
		JSONObject ret = new JSONObject();
		ret.put("jobId", jobId);
		ret.put("success", success);
		ret.put("lat", this.point.getLat());
		ret.put("lon", this.point.getLon());
		return ret.toString();
	}

}
