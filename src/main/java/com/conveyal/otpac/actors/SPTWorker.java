	package com.conveyal.otpac.actors;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.ResultSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.profile.ProfileResponse;
import org.opentripplanner.profile.AnalystProfileRouterPrototype;
import org.opentripplanner.routing.algorithm.EarliestArrivalSPTService;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.spt.ShortestPathTree;
import org.opentripplanner.standalone.Router;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.otpac.PointSetDatastore;
import com.conveyal.otpac.message.AnalystClusterRequest;
import com.conveyal.otpac.message.GetGraphAndSamples;
import com.conveyal.otpac.message.GraphAndSampleSet;
import com.conveyal.otpac.message.OneToManyProfileRequest;
import com.conveyal.otpac.message.OneToManyRequest;
import com.conveyal.otpac.message.SetOneToManyContext;
import com.conveyal.otpac.message.WorkResult;

public class SPTWorker extends UntypedActor {

	private Router router;
	
	/** the graph building actor */
	private ActorRef graphBuilder;
	
	/** the current sampleset */
	private SampleSet sampleSet;
	
	/** the current point set id for the sampleset */
	private String pointsetId;
	
	public SPTWorker (ActorRef graphBuilder) {
		this.graphBuilder = graphBuilder;
	}
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	@Override
	public void onReceive(Object message) throws Exception {
		if( message instanceof OneToManyRequest ){
			onMsgOneToManyRequest((OneToManyRequest) message);
		} else if (message instanceof OneToManyProfileRequest) {
			onMsgOneToManyProfileRequest((OneToManyProfileRequest) message);
		} else {
			unhandled(message);
		}
	}

	private boolean checkGraph (AnalystClusterRequest req) throws Exception {
		if (this.router == null || !this.router.id.equals(req.graphId) ||
				this.sampleSet == null || !this.pointsetId.equals(req.destinationPointsetId)) {
			// get the graph
			// this shouldn't build the graph; rather, it should be getting a graph from memory
			// on the graphbuilder, which is in the local JVM so should work via reference passing.
			// the graph should be built by WorkerManager
			// however, in the interests of being resilient, we give graph building a very long timeout
			// if the graph builder gets restarted at some point, this will actually build the graph
			// and nobody is counting on this thread to do anything; if a few workresults get backed up,
			// no big deal; they'll just be retried.
			// however, we probably ought to handle OTP throwing an exception on graph build as this is not
			// likely to be a recoverable situation. Then again, it could be an S3 error.
			Timeout t = new Timeout(Duration.create(10, "minutes"));
			GetGraphAndSamples gg = new GetGraphAndSamples(req.graphId, req.destinationPointsetId);
			Future<Object> future = Patterns.ask(graphBuilder, gg, t);
			GraphAndSampleSet g = (GraphAndSampleSet) Await.result(future, t.duration());
			
			this.router = g.router;
			this.sampleSet = g.sampleSet;
			this.pointsetId = g.pointsetId;
		}
		
		if (this.router == null || !this.router.id.equals(req.graphId) ||
				this.sampleSet == null || !this.pointsetId.equals(req.destinationPointsetId)) {
			log.error("graph build did not return correct graph and sampleset!");
			log.error("expected: " + req.graphId + " / " + req.destinationPointsetId +
					", got " + (this.router == null ? "null" : this.router.id) + " / " + this.pointsetId);
			
			return false;
		}
		
		return true;
	}
	
	private void onMsgOneToManyRequest(OneToManyRequest req) throws Exception {
		log.debug("got req {}", req);
		
		// check/get the right graph and sample set, or fail if we can't
		if (!checkGraph(req)) return;
		
		try{
			req.options.setRoutingContext(this.router.graph);
		} catch ( VertexNotFoundException ex ) {
			log.debug("could not find origin vertex {}", req.options.from);
			WorkResult res = new WorkResult(false, null, req.from, req.jobId);
			getSender().tell(res, getSelf());
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
			
			ResultSet ind = new ResultSet(sampleSet, ts);
			ind.id = req.from.getId();

			WorkResult res = new WorkResult(true, ind, req.from, req.jobId);
			getSender().tell(res, getSelf());
		}
		catch(Exception e) {
			log.debug("failed to calc timesurface for feature {}", req.from.getId());
			WorkResult res = new WorkResult(false, null, req.from, req.jobId);
			getSender().tell(res, getSelf());
		}
	}
	
	/** Perform profile routing 
	 * @throws Exception */
	private void onMsgOneToManyProfileRequest(OneToManyProfileRequest message) throws Exception {
		// check/get the right graph and sample set, or fail if we can't
		if (!checkGraph(message)) return;
		
		AnalystProfileRouterPrototype rtr;
		try {
			rtr = new AnalystProfileRouterPrototype(this.router.graph, message.options);
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null, null, message.from, message.jobId), getSelf());
			return;
		}
		
		try {
			rtr.route();

			TimeSurface.RangeSet result = rtr.route();			

			ResultSet bestCase = new ResultSet(sampleSet, result.min);
			bestCase.id = message.from.getId();

			ResultSet avgCase = new ResultSet(sampleSet, result.avg);
			avgCase.id = message.from.getId();
			
			ResultSet worstCase = new ResultSet(sampleSet, result.max);
			worstCase.id = message.from.getId();
			
			// TODO: Central tendency calculation
			WorkResult result1 = new WorkResult(true, bestCase, avgCase, worstCase, null, message.from, message.jobId);
			getSender().tell(result1, getSelf());
		} catch (Exception e) {
			log.debug("failed to calc timesurface for feature {}", message.from.getId());
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			WorkResult res = new WorkResult(false, null, null, null, null, message.from, message.jobId);
			getSender().tell(res, getSelf());
		}
		finally {
			rtr.cleanup();
		}
	}
}