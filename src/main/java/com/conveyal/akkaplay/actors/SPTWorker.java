package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.opentripplanner.analyst.Indicator;
import org.opentripplanner.analyst.IndicatorLite;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.PointSet.AttributeData;
import org.opentripplanner.analyst.PointSet.Category;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.graph.Vertex;
import org.opentripplanner.routing.spt.GraphPath;
import org.opentripplanner.routing.spt.ShortestPathTree;

import com.conveyal.akkaplay.WorkResultCompiler;
import com.conveyal.akkaplay.message.*;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class SPTWorker extends UntypedActor {

	private Graph graph;
	private SampleSet to;
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	SPTWorker() {
	}

	@Override
	public void onReceive(Object message) {
		if( message instanceof SetOneToManyContext ) {
			onSetContext(message);
		} else if( message instanceof OneToManyRequest ){
			onRequest(message);
		} else {
			unhandled(message);
		}
	}

	private void onRequest(Object message) {
		OneToManyRequest req = (OneToManyRequest)message;
		log.debug("got req {}", req);
		
		RoutingRequest rr = new RoutingRequest();
		rr.setBatch(true);
		GenericLocation fromLoc = new GenericLocation(req.from.getLat(), req.from.getLon());
		rr.setFrom(fromLoc);
		rr.setDateTime( req.date );
		try{
			rr.setRoutingContext(this.graph);
		} catch ( VertexNotFoundException ex ) {
			log.debug("could not find origin vertex {}", fromLoc);
			getSender().tell(new WorkResult(false, null), getSelf());
			return;
		}
		
		EarliestArrivalSPTService algo = new EarliestArrivalSPTService();
		algo.setMaxDuration( 60*60 );
		
		long d0 = System.currentTimeMillis();
		ShortestPathTree spt = algo.getShortestPathTree(rr);
		long d1 = System.currentTimeMillis();
		log.debug("got spt, vertexcount={} in {} ms", spt.getVertexCount(), d1-d0 );
		
		TimeSurface ts = new TimeSurface( spt );
		
		IndicatorLite ind = new IndicatorLite(this.to, ts, false);
		
		WorkResult res = new WorkResult(true, ind);
		res.point = req.from;
		getSender().tell(res, getSelf());
	}

	private void onSetContext(Object message) {
		SetOneToManyContext ctx = (SetOneToManyContext)message;
		
		log.debug("setting 1-many context: {}", ctx);
		log.debug("setting context graph: {}", ctx.graph);
		
		this.graph = ctx.graph;
		this.to = ctx.to;
		
		getSender().tell(new Boolean(true), getSelf());
	}

}