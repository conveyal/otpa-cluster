package com.conveyal.akkaplay.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	public String gtfs_path;
	public String osm_path;
	public int jobId;

	public JobSpec(String gtfs_path, String osm_path) {
		this.gtfs_path = gtfs_path;
		this.osm_path = osm_path;
	}

}
