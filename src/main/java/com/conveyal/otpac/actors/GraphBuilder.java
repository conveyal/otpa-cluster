package com.conveyal.otpac.actors;

import java.util.HashMap;
import java.util.Map;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.services.GraphService;
import org.opentripplanner.standalone.Router;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.conveyal.otpac.ClusterGraphService;
import com.conveyal.otpac.PointSetDatastore;
import com.conveyal.otpac.message.GetGraph;
import com.conveyal.otpac.message.GetGraphAndSamples;
import com.conveyal.otpac.message.GraphAndSampleSet;

public class GraphBuilder extends UntypedActor {

	private GraphService graphService;
	
	/**
	 * We cache the router locally, so that we can be sure that samplesets and
	 * routers always match.
	 */
	private Router router = null;
	
	/**
	 * Cache of samplesets for the current router.
	 */
	private Map<String, SampleSet> sampleSetCache = new HashMap<String, SampleSet>();
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	/** from whence the pointsets come */
	private PointSetDatastore pointsetCache;

	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof GetGraphAndSamples)
			onMsgGetGraphAndSamples((GetGraphAndSamples) msg);
		else if (msg instanceof GetGraph)
			onMsgGetGraph((GetGraph) msg);
	}
	
	public GraphBuilder(String s3ConfigFilename, Boolean workOffline, String graphsBucket, String pointsetsBucket) {
		this.graphService = new ClusterGraphService(s3ConfigFilename, workOffline, graphsBucket);
		this.pointsetCache = new PointSetDatastore(10, s3ConfigFilename, workOffline, pointsetsBucket);
	}
	
	private void buildGraphIfNeeded (String graphId) {
		if (this.router == null || !this.router.id.equals(graphId)) {		
			router = graphService.getRouter(graphId);
			// clear the sample set cache as they don't match anything anymore
			sampleSetCache.clear();
		}
	}
	
	private void onMsgGetGraphAndSamples(GetGraphAndSamples msg) throws Exception {
		buildGraphIfNeeded(msg.graphId);
		
		if (!sampleSetCache.containsKey(msg.pointsetId)) {
			// build the sample set
			PointSet ps = this.pointsetCache.get(msg.pointsetId);
			SampleSet ss = ps.getSampleSet(this.router.graph);
			sampleSetCache.put(msg.pointsetId, ss);
		}
				
		getSender().tell(new GraphAndSampleSet(router, sampleSetCache.get(msg.pointsetId), msg.pointsetId), getSelf());
	}
	
	private void onMsgGetGraph(GetGraph msg) {
		buildGraphIfNeeded(msg.graphId);
		
		getSender().tell(router, getSelf());
	}
}
