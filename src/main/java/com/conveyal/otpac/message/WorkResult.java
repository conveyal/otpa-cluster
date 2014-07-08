package com.conveyal.otpac.message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.opentripplanner.analyst.Histogram;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.ResultFeature;

import com.bedatadriven.geojson.GeometrySerializer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Geometry;


public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public boolean success;
	public PointFeature point=null;
	public int jobId;
	private ResultFeature feat;

	public WorkResult(boolean success, ResultFeature feat) {
		this.success = success;
		this.feat = feat;
	}
	
	public String toString(){
		if(success)
			return "<Job success:"+success+" point:"+point+" histograms.size:"+feat.histograms.size()+">";
		else
			return "<Job success:"+success+">";
	}

	public String toJsonString() throws IOException {
		
		JsonFactory jsonFactory = new JsonFactory();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		JsonGenerator jgen = jsonFactory.createGenerator(out);
		jgen.setCodec(new ObjectMapper());
		
		jgen.writeStartObject();
		{
			jgen.writeNumberField("jobId", jobId);
			jgen.writeBooleanField("success", success);
			if(success){
				jgen.writeNumberField("lat", this.point.getLat());
				jgen.writeNumberField("lon", this.point.getLon());
				writeHistogramsJson(jgen,this.feat.histograms);
				Geometry geom = this.point.getGeom();
				if(geom!=null){
					GeometrySerializer gs = new GeometrySerializer();
					jgen.writeFieldName("geom");
					gs.writeGeometry(jgen, geom);
				}
			}
		}
		jgen.writeEndObject();
		
		return out.toString();
	}

	private void writeHistogramsJson(JsonGenerator jgen, Map<String, Histogram> histograms) throws JsonGenerationException, IOException {
		jgen.writeObjectFieldStart("histograms");
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
