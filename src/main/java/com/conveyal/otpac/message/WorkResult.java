package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opentripplanner.analyst.Histogram;
import org.opentripplanner.analyst.Indicator;
import org.opentripplanner.analyst.IndicatorLite;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet.Attribute;
import org.opentripplanner.analyst.PointSet.Category;
import org.opentripplanner.analyst.Quantiles;


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
			ret.put("categories", getCategoriesJson(this.indicator.categories));
		}
		return ret.toString();
	}

	private JSONObject getCategoriesJson(Map<String, Category> categories) {
		JSONObject ret = new JSONObject();
		
		for(Entry<String,Category> entry : categories.entrySet()){
			ret.put(entry.getKey(), getCategoryJson(entry.getValue()));
		}
		
		return ret;
	}

	private JSONObject getCategoryJson(Category value) {
		JSONObject ret = new JSONObject();
		
		for(Entry<String,Attribute> entry : value.getAttributes().entrySet()){
			ret.put(entry.getKey(), getAttributeJson(entry.getValue()));
		}
		
		return ret;
	}

	private JSONArray getAttributeJson(Attribute attr) {
		JSONArray ret = new JSONArray();
		
		for(Histogram hist: attr.getHistogram()){
			ret.put( hist.sums );
		}
		
		return ret;
	}

}
