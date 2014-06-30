package com.conveyal.akkaplay.actors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.opentripplanner.analyst.Indicator;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.PointSet.AttributeData;
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
			SetOneToManyContext ctx = (SetOneToManyContext)message;
			
			log.debug("setting 1-many context: {}", ctx);
			
			this.graph = ctx.graph;
			this.to = ctx.to;
			
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
			Indicator ind = new Indicator(this.to, ts, false);
			
//			WorkResultCompiler comp = new WorkResultCompiler();
//			//TODO make the pointset api do this
//			for(int i=0; i<this.to.featureCount(); i++){
//				PointFeature loc = this.to.getFeature(i);
//				Vertex vtx = this.toVertices.get(i);
//				
//				if(vtx==null){
//					continue;
//				}
//				
//				GraphPath path = spt.getPath(vtx, true);
//				if(path==null){
//					continue;
//				}
//				
//				int dur = path.getDuration();
//				
//				for( AttributeData ind : loc.getAttributes() ){
//					comp.put( ind, dur );
//				}
//				
//			}
			
			WorkResult res = new WorkResult(true, ind);
			getSender().tell(res, getSelf());
		} else {
			unhandled(message);
		}
	}

}