package com.conveyal.otpac.message;

import java.io.Serializable;

public class BuildGraph implements Serializable {
	public String graphId;

	public BuildGraph(String graphId) {
		this.graphId = graphId;
	}

}
