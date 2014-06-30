package com.conveyal.akkaplay.message;

import java.io.Serializable;

import org.json.JSONObject;
import org.opentripplanner.analyst.Indicator;
import org.opentripplanner.analyst.IndicatorLite;
import org.opentripplanner.analyst.PointFeature;


public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public PointFeature point=null;
	public int jobId;
	private IndicatorLite indicator;

	public WorkResult(boolean success, IndicatorLite ind) {
		this.success = success;
		this.indicator = ind;
	}
	
	public String toString(){
		if(success)
			return "<Job success:"+success+" point:"+point+" histograms.size:"+indicator.featureCount()+">";
		else
			return "<Job success:"+success+">";
	}

	public String toJsonString() {
		JSONObject ret = new JSONObject();
		ret.put("jobId", jobId);
		ret.put("success", success);
		if(success){
			ret.put("lat", this.point.getLat());
			ret.put("lon", this.point.getLon());
		}
		return ret.toString();
	}

}
