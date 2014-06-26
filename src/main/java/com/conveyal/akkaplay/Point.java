package com.conveyal.akkaplay;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

public class Point implements Serializable {

	private static final long serialVersionUID = -8244901603146220240L;
	
	private Float lat;
	private Float lon;
	private Map<String, Float> props;

	Point() {
		lat = null;
		lon = null;
		props = new HashMap<String, Float>();
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

	public void setProp(String key, float val) {
		this.props.put(key, val);
	}

	public float getLon() {
		return lon;
	}

	public double getLat() {
		return lat;
	}

	public List<Indicator> getIndicators() {
		ArrayList<Indicator> ret = new ArrayList<Indicator>();
		for( Entry<String,Float> entry : this.props.entrySet() ){
			ret.add( new Indicator(entry.getKey(), entry.getValue()) );
		}
		return ret;
	}
	
	public String toString(){
		String ret = "<Point ("+this.lat+","+this.lon+") ";
		for(Entry<String,Float> entry : this.props.entrySet()){
			ret += entry.getKey()+":"+entry.getValue()+" ";
		}
		return ret;
	}

	public JSONObject toJSONObject() {
		JSONObject ret = new JSONObject();
		float[] coord = {this.lat,this.lon};
		ret.put("coord", coord);
		return ret;
	}

}
