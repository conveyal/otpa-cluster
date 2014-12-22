	package com.conveyal.otpac.actors;

import org.opentripplanner.analyst.ResultSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.profile.ProfileResponse;
import org.opentripplanner.profile.ProfileRouter;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.spt.ShortestPathTree;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.conveyal.otpac.message.OneToManyProfileRequest;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.WorkResult;

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
		} else if (message instanceof OneToManyProfileRequest) {
			onMsgOneToManyProfileRequest((OneToManyProfileRequest) message);
		} else {
			unhandled(message);
		}
	}

	private void onMsgOneToManyRequest(OneToManyRequest req) {
		log.debug("got req {}", req);
		
		try{
			req.options.setRoutingContext(this.graph);
		} catch ( VertexNotFoundException ex ) {
			log.debug("could not find origin vertex {}", req.options.from);
			getSender().tell(new WorkResult(false, null), getSelf());
			return;
		}
		
		try {
			EarliestArrivalSPTService algo = new EarliestArrivalSPTService();
			algo.maxDuration = 60*60;
			
			long d0 = System.currentTimeMillis();
			ShortestPathTree spt = algo.getShortestPathTree(req.options);
			
			req.options.cleanup();
			
			long d1 = System.currentTimeMillis();
			log.debug("got spt, vertexcount={} in {} ms", spt.getVertexCount(), d1-d0 );
			
			TimeSurface ts = new TimeSurface( spt );
			
			ResultSet ind = new ResultSet(this.to, ts);
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
	
	/** Perform profile routing */
	private void onMsgOneToManyProfileRequest(OneToManyProfileRequest message) {
		ProfileRouter rtr;
		try {
			rtr = new ProfileRouter(this.graph, message.options);
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null), getSelf());
			return;
		}
		
		try {
			rtr.route();
			ResultSet bestCase = new ResultSet(this.to, rtr.minSurface);
			bestCase.id = message.from.getId();
			
			ResultSet worstCase = new ResultSet(this.to, rtr.maxSurface);
			worstCase.id = message.from.getId();
			
			// TODO: Central tendency calculation
			WorkResult result = new WorkResult(true, bestCase, worstCase, null);
			getSender().tell(result, getSelf());
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null), getSelf());
		}
		finally {
			rtr.cleanup();
		}
	}

	private void onMsgSetOneToManyContext(SetOneToManyContext ctx) {		
		log.debug("setting 1-many context: {}", ctx);
		log.debug("setting context graph: {}", ctx.graph);
		
		this.graph = ctx.graph;
		this.to = ctx.to;
		
		getSender().tell(new Boolean(true), getSelf());
	}
}