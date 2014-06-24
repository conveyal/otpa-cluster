package com.conveyal.akkaplay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.conveyal.akkaplay.message.WorkResult;

public class WorkResultCompiler {
	class Particle {
		public float indval;
		public int dur;
		
		public Particle(float val, int dur) {
			this.indval = val;
			this.dur = dur;
		}
	}

	Map<String, List<Float>> histograms = new HashMap<String, List<Float>>();

	public void put(Indicator ind, int dur) {
		if (!histograms.containsKey(ind.name)) {
			histograms.put(ind.name, new ArrayList<Float>());
		}
		List<Float> histogram = histograms.get(ind.name);
		
		//expand the histogram if necessary
		if(histogram.size()<(dur+1)){
			int futureSize = dur+1;
			int expansionSize = futureSize-histogram.size();
			for(int i=0; i<expansionSize; i++){
				histogram.add(0.0f);
			}
		}
		
		histogram.set(dur, histogram.get(dur)+ind.val);
	}

	public WorkResult getWorkResult() {
		WorkResult ret = new WorkResult(true);
		for(Entry<String, List<Float>> entry : histograms.entrySet() ){
			ret.addHistogram( entry.getKey(), entry.getValue() );
		}
		return ret;
	}

}
