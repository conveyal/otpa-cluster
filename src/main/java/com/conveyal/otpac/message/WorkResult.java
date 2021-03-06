package com.conveyal.otpac.message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.opentripplanner.analyst.Histogram;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.ResultSet;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Geometry;


public class WorkResult implements Serializable {

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public PointFeature point=null;
	public int jobId;
	
	/** The result, or the central tendency of the results in profile mode */
	private ResultSet pointEstimate;
	private ResultSet bestCase;
	private ResultSet worstCase;
	
	/** Was this request made in profile mode? */
	public final boolean profile;

	public WorkResult(boolean success, ResultSet feat, PointFeature point, int jobId) {
		this.success = success;
		this.pointEstimate = feat;
		this.bestCase = null;
		this.worstCase = null;
		this.point = point;
		this.jobId = jobId;
		this.profile = false;
	}
	
	public WorkResult(boolean success, ResultSet bestCase,  ResultSet pointEstimate, ResultSet worstCase, PointFeature point, int jobId) {
		this.success = success;
		this.pointEstimate = pointEstimate;
		this.bestCase = bestCase;
		this.worstCase = worstCase;
		this.point = point;
		this.jobId = jobId;
		this.profile = true;
	}
	
	public String toString(){
		if(success)
			return "<Job success:"+success+" point:"+point+" histograms.size:"+ (profile ? bestCase : pointEstimate).histograms.size()+">";
		else
			return "<Job success:"+success+">";
	}
	
	/** The result, or the central tendency of the results in profile mode */
	public ResultSet getPointEstimate() {
		return pointEstimate;
	}
	
	/** The best-case result in profile mode */
	public ResultSet getBestCase () {
		return bestCase;
	}

	/** The worst-case result in profile mode */
	public ResultSet getWorstCase () {
		return worstCase;
	}

	public String toJsonString() throws IOException {
		
		JsonFactory jsonFactory = new JsonFactory();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		JsonGenerator jgen = jsonFactory.createGenerator(out);
		jgen.setCodec(new ObjectMapper());
		ObjectMapper geomSerializer = new ObjectMapper();
		
		jgen.writeStartObject();
		{
			jgen.writeNumberField("jobId", jobId);
			jgen.writeBooleanField("success", success);
			jgen.writeBooleanField("profile", profile);
			if(success){
				jgen.writeNumberField("lat", this.point.getLat());
				jgen.writeNumberField("lon", this.point.getLon());
				
				if (profile) {
					writeHistogramsJson("bestCase", jgen, this.bestCase.histograms);
					writeHistogramsJson("worstCase", jgen, this.worstCase.histograms);
				}
				else {
					writeHistogramsJson("histograms", jgen, this.pointEstimate.histograms);
				}
				
				Geometry geom = this.point.getGeom();
				if(geom != null){
					jgen.writeFieldName("geom");
					geomSerializer.writeValue(jgen, geom);
				}
			}
		}
		jgen.writeEndObject();
		
		jgen.flush();
		
		return out.toString();
	}

	private void writeHistogramsJson(String name, JsonGenerator jgen, Map<String, Histogram> histograms)
			throws JsonGenerationException, IOException {
		jgen.writeObjectFieldStart(name);
		{
			for(Entry<String,Histogram> entry : histograms.entrySet()){
				jgen.writeArrayFieldStart( entry.getKey() );
				{
					int[] sums = entry.getValue().sums;
					for(int i=0; i<sums.length; i++){
						jgen.writeNumber(sums[i]);
					}
				}
				jgen.writeEndArray();
			}
		}
		jgen.writeEndObject();
	}

}
