package com.conveyal.otpac.actors;


import java.util.TimeZone;

import org.opentripplanner.analyst.ResultFeature;
import org.opentripplanner.analyst.ResultFeatureWithTimes;
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
		
		RoutingRequest rr = new PrototypeAnalystRequest();
		rr.batch = true;
		GenericLocation fromLoc = new GenericLocation(req.from.getLat(), req.from.getLon());
		rr.from = fromLoc;
		rr.setDateTime( req.date );
		
		rr.modes.clear();
		switch(req.mode) {
			case "TRANSIT":
				rr.modes.setWalk(true);
				rr.modes.setTransit(true);
				break;
			case "CAR,TRANSIT,WALK":
				rr.modes.setCar(true);
				rr.modes.setTransit(true);
				rr.modes.setWalk(true);
				rr.kissAndRide = true;
				rr.walkReluctance = 1.0;
				break;	
			case "BIKE,TRANSIT":
				rr.modes.setBicycle(true);
				rr.modes.setTransit(true);
				break;
			case "CAR":
				rr.modes.setCar(true);
				break;
			case "BIKE":
				rr.modes.setBicycle(true);
				break;
			case "WALK":
				rr.modes.setWalk(true);
				break;
		}
		
		try{
			rr.setRoutingContext(this.graph);
		} catch ( VertexNotFoundException ex ) {
			log.debug("could not find origin vertex {}", fromLoc);
			getSender().tell(new WorkResult(false, null), getSelf());
			return;
		}
		
		try {
			EarliestArrivalSPTService algo = new EarliestArrivalSPTService();
			algo.maxDuration = 60*60;
			
			long d0 = System.currentTimeMillis();
			ShortestPathTree spt = algo.getShortestPathTree(rr);
			
			rr.cleanup();
			
			long d1 = System.currentTimeMillis();
			log.debug("got spt, vertexcount={} in {} ms", spt.getVertexCount(), d1-d0 );
			
			TimeSurface ts = new TimeSurface( spt );
			
			ResultFeature ind = new ResultFeature(this.to, ts);
			ind.id = req.from.getId();

			WorkResult res = new WorkResult(true, ind);
			res.point = req.from;
			getSender().tell(res, getSelf());
		}
		catch(Exception e) {
			log.debug("failed to calc timesurface for feature {}", req.from.getId());
			getSender().tell(new WorkResult(false, null), getSelf());
		}
	}

	private void onMsgSetOneToManyContext(SetOneToManyContext ctx) {		
		log.debug("setting 1-many context: {}", ctx);
		log.debug("setting context graph: {}", ctx.graph);
		
		this.graph = ctx.graph;
		this.to = ctx.to;
		
		getSender().tell(new Boolean(true), getSelf());
	}
	
	public class PrototypeAnalystRequest extends RoutingRequest {
		
		private static final long serialVersionUID = 1L;
		static final int MAX_TIME = 7200;

	    public PrototypeAnalystRequest() {
	    	
			 this.maxWalkDistance = 40000;
			 this.clampInitialWait = 1800;
			 this.modes.setWalk(true);

			 this.arriveBy = false;
			 this.batch = true;
			 this.worstTime = this.dateTime + (this.arriveBy ? - (MAX_TIME + this.clampInitialWait): (MAX_TIME + this.clampInitialWait));
	    }
	    
	}

}