package com.conveyal.akkaplay.message;

import com.conveyal.akkaplay.JobResultsApplication;

public class SetStatusServer {
	public JobResultsApplication statusServer;

	public SetStatusServer(JobResultsApplication app) {
		this.statusServer = app;
	}

}
