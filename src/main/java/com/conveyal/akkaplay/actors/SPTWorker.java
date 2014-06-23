package com.conveyal.akkaplay.actors;

import java.util.Date;

import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.spt.ShortestPathTree;

import com.conveyal.akkaplay.message.*;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class SPTWorker extends UntypedActor {

	private Graph graph;
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	SPTWorker() {
	}

	@Override
	public void onReceive(Object message) {
		if( message instanceof SetGraph ) {
			log.debug("setting graph: {}", ((SetGraph)message).graph);
			
			this.graph = ((SetGraph)message).graph;
			getSender().tell(new Boolean(true), getSelf());
		} else if( message instanceof OneToManyRequest ){
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
				getSender().tell(new WorkResult(false), getSelf());
				return;
			}
			
			EarliestArrivalSPTService algo = new EarliestArrivalSPTService();
			algo.setMaxDuration( 60*60 );
			
//			long d0 = System.currentTimeMillis();
//			ShortestPathTree spt = algo.getShortestPathTree(rr);
//			long d1 = System.currentTimeMillis();
//			log.debug("got spt, vertexcount={} in {} ms", spt.getVertexCount(), d1-d0 );
			
			getSender().tell(new WorkResult(true), getSelf());
		} else {
			unhandled(message);
		}
	}

}