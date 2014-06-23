package com.conveyal.akkaplay;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Point implements Serializable {

	private static final long serialVersionUID = -8244901603146220240L;
	
	private Float lat;
	private Float lon;
	private Map<String, String> props;

	Point() {
		lat = null;
		lon = null;
		props = new HashMap<String, String>();
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

	public void setProp(String key, String val) {
		this.props.put(key, val);
	}

	public float getLon() {
		return lon;
	}

	public double getLat() {
		return lat;
	}

}
