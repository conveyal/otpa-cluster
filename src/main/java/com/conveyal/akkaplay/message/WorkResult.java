package com.conveyal.akkaplay.message;

import java.io.Serializable;

public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;

	public WorkResult(boolean success) {
		this.success = success;
	}
	
	public String toString(){
		return "<Job success:"+success+">";
	}

}
