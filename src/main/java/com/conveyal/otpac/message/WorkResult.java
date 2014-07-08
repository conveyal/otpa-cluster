package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opentripplanner.analyst.Histogram;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.ResultFeature;


public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public PointFeature point=null;
	public int jobId;
	private ResultFeature feat;

	public WorkResult(boolean success, ResultFeature feat) {
		this.success = success;
		this.feat = feat;
	}
	
	public String toString(){
		if(success)
			return "<Job success:"+success+" point:"+point+" histograms.size:"+feat.histograms.size()+">";
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
			ret.put("histograms", getPropertiesJson(this.feat.histograms));
		}
		return ret.toString();
	}

	private JSONObject getPropertiesJson(Map<String, Histogram> categories) {
		JSONObject ret = new JSONObject();
		
		for(Entry<String,Histogram> entry : categories.entrySet()){
			ret.put(entry.getKey(), entry.getValue().sums);
		}
		
		return ret;
	}

}
