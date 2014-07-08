package com.conveyal.otpac.actors;


import org.opentripplanner.analyst.ResultFeature;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.spt.ShortestPathTree;

import com.conveyal.otpac.message.*;

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
			onMsgSetOneToManyContext((SetOneToManyContext) message);
		} else if( message instanceof OneToManyRequest ){
			onMsgOneToManyRequest((OneToManyRequest) message);
		} else {
			unhandled(message);
		}
	}

	private void onMsgOneToManyRequest(OneToManyRequest req) {
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
		
		ResultFeature ind = ResultFeature.eval(this.to, ts);
		
		WorkResult res = new WorkResult(true, ind);
		res.point = req.from;
		getSender().tell(res, getSelf());
	}

	private void onMsgSetOneToManyContext(SetOneToManyContext ctx) {		
		log.debug("setting 1-many context: {}", ctx);
		log.debug("setting context graph: {}", ctx.graph);
		
		this.graph = ctx.graph;
		this.to = ctx.to;
		
		getSender().tell(new Boolean(true), getSelf());
	}

}