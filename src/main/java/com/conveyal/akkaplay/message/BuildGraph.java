package com.conveyal.akkaplay.message;

public class BuildGraph {
	public String gtfs_path;
	public String osm_path;

	public BuildGraph(String gtfs_path, String osm_path) {
		this.gtfs_path = gtfs_path;
		this.osm_path = osm_path;
	}

}
