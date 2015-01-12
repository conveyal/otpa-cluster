package com.conveyal.otpac.actors;

import org.opentripplanner.analyst.ResultSet;
import org.opentripplanner.analyst.ResultSetWithTimes;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.profile.ProfileResponse;
import org.opentripplanner.profile.AnalystProfileRouterPrototype;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.spt.ShortestPathTree;
import org.opentripplanner.standalone.Router;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.conveyal.otpac.message.OneToManyProfileRequest;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.WorkResult;

public class SPTWorker extends UntypedActor {

	private Router router;
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
			req.options.setRoutingContext(this.router.graph);
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
			
			// temporarily use environment to decide whether to include times
			// or not.
			if (context().system().settings().config().getBoolean("includeTimes")) {
				log.info("including times in result");
				
				ResultSet ind = new ResultSetWithTimes(this.to, ts);
				ind.id = req.from.getId();
	
				WorkResult res = new WorkResult(true, ind);
				res.point = req.from;
				getSender().tell(res, getSelf());
			}
			else {
				ResultSet ind = new ResultSet(this.to, ts);
				ind.id = req.from.getId();

				WorkResult res = new WorkResult(true, ind);
				res.point = req.from;
				getSender().tell(res, getSelf());
			}
		}
		catch(Exception e) {
			log.debug("failed to calc timesurface for feature {}", req.from.getId());
			getSender().tell(new WorkResult(false, null), getSelf());
		}
	}
	
	/** Perform profile routing */
	private void onMsgOneToManyProfileRequest(OneToManyProfileRequest message) {
		AnalystProfileRouterPrototype rtr;
		try {
				rtr = new AnalystProfileRouterPrototype(this.router.graph, message.options);
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null, null), getSelf());
			return;
		}
		
		try {
			TimeSurface.RangeSet result = rtr.route();

			// temporarily use environment to decide whether to include times
			// or not.
			if (context().system().settings().config().getBoolean("includeTimes")) {
				log.info("Including times in result");
				ResultSet bestCase = new ResultSetWithTimes(this.to, result.min);
				bestCase.id = message.from.getId();
	
				ResultSet avgCase = new ResultSetWithTimes(this.to, result.avg);
				avgCase.id = message.from.getId();
				
				ResultSet worstCase = new ResultSetWithTimes(this.to, result.max);
				worstCase.id = message.from.getId();
	
				WorkResult result1 = new WorkResult(true, bestCase, avgCase, worstCase, null);
				getSender().tell(result1, getSelf());
			}
			else {
				
				ResultSet bestCase = new ResultSet(this.to, result.min);
				bestCase.id = message.from.getId();

				ResultSet avgCase = new ResultSet(this.to, result.avg);
				avgCase.id = message.from.getId();
				
				ResultSet worstCase = new ResultSet(this.to, result.max);
				worstCase.id = message.from.getId();

				WorkResult result1 = new WorkResult(true, bestCase, avgCase, worstCase, null);
				getSender().tell(result1, getSelf());
			}
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null, null), getSelf());
		}
		finally {
			rtr.cleanup();
		}
	}

	private void onMsgSetOneToManyContext(SetOneToManyContext ctx) {		
		log.debug("setting 1-many context: {}", ctx);
		log.debug("setting context router: {}", ctx.router);
		
		this.router = ctx.router;
		this.to = ctx.to;
		
		getSender().tell(new Boolean(true), getSelf());
	}
}