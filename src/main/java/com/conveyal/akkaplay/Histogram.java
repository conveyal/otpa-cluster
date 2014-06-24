package com.conveyal.akkaplay;

import java.io.Serializable;
import java.util.List;

public class Histogram implements Serializable{
	private static final long serialVersionUID = 4233847114005013760L;
	
	public String name;
	public float[] bins;
	
	public Histogram(String name, List<Float> bins){
		this.bins = new float[bins.size()];
		for(int i=0; i<bins.size(); i++){
			this.bins[i] = bins.get(i);
		}
		this.name = name;
	}
}
