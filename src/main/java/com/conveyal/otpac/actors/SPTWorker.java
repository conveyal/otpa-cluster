package com.conveyal.otpac.actors;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.conveyal.otpac.message.*;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.ResultSet;
import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.analyst.TimeSurface;
import org.opentripplanner.profile.IsochroneGenerator;
import org.opentripplanner.profile.RepeatedRaptorProfileRouter;
import org.opentripplanner.routing.algorithm.AStar;
import org.opentripplanner.routing.error.VertexNotFoundException;
import org.opentripplanner.routing.spt.ShortestPathTree;
import org.opentripplanner.standalone.Router;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

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
				this.pointsetId != null && !this.pointsetId.equals(req.destinationPointsetId) ||
				this.pointsetId == null && req.destinationPointsetId != null) {
			// get the graph
			// this shouldn't build the graph; rather, it should be getting a graph from memory
			// on the graphbuilder, which is in the local JVM so should work via reference passing.
			// the graph should be built by WorkerManager
			// However, building a SampleSet can take some time, so give it a bit of a long timeout.
			Timeout t = new Timeout(Duration.create(90, "seconds"));
			
			if (req.destinationPointsetId != null) {
				GetGraphAndSamples gg = new GetGraphAndSamples(req.graphId, req.destinationPointsetId);
				Future<Object> future = Patterns.ask(graphBuilder, gg, t);
				GraphAndSampleSet g;
				try {
					g = (GraphAndSampleSet) Await.result(future, t.duration());
				} catch (TimeoutException e) {
					return false;
				}
				
				this.router = g.router;
				this.sampleSet = g.sampleSet;
				this.pointsetId = g.pointsetId;
			}
			
			else {
				GetGraph gg = new GetGraph(req.graphId);
				Future<Object> future = Patterns.ask(graphBuilder, gg, t);
				Router g;
				try {
					g = (Router) Await.result(future, t.duration());
				} catch (TimeoutException e) {
					return false;
				}
				
				this.router = g;
				// TODO cache
				// TODO error kernel: should this be in the graph builder?
				PointSet ps = PointSet.regularGrid(g.graph.getExtent(), IsochroneGenerator.GRID_SIZE_METERS);
				this.sampleSet = new SampleSet(ps, g.graph.getSampleFactory());
				this.pointsetId = null;
			}
			

		}
		
		if (this.router == null || !this.router.id.equals(req.graphId)) {
			log.error("graph build did not return correct graph!");
			return false;
		}
		
		if (req.destinationPointsetId != null &&
				(this.sampleSet == null || !this.pointsetId.equals(req.destinationPointsetId))) {
			log.error("graph build did not return correct sampleset!");
			return false;
		}
		
		return true;
	}
	
	private void onMsgOneToManyRequest(OneToManyRequest req) throws Exception {
		log.debug("got req {}", req);
		
		// check/get the right graph and sample set, or fail if we can't
		if (!checkGraph(req)) {
			log.error("unable to get graph %s", req.graphId);
			getSender().tell(new ResultFailed(), getSelf());
			return;
		}
		
		try{
			req.options.setRoutingContext(this.router.graph);
		} catch ( VertexNotFoundException ex ) {
			log.debug("could not find origin %s", req.options.from);
			WorkResult res = new WorkResult(false, null, req.from, req.jobId);
			getSender().tell(res, getSelf());
			return;
		} catch (Exception e) {
			log.error("Failed to set routing context %s", req.graphId);
			WorkResult res = new WorkResult(false, null, req.from, req.jobId);
			getSender().tell(res, getSelf());
			return;
		}
		
		try {
			AStar astar = new AStar();
			long d0 = System.currentTimeMillis();
			ShortestPathTree spt = astar.getShortestPathTree(req.options, 10); 
			
			req.options.cleanup();
			
			long d1 = System.currentTimeMillis();
			log.debug("got spt, vertexcount={} in {} ms", spt.getVertexCount(), d1-d0 );
			
			TimeSurface ts = new TimeSurface( spt );
			
			ResultSet ind;
			
			if (req.destinationPointsetId != null)
				ind = new ResultSet(sampleSet, ts, req.includeTimes, false);
			else
				ind = new ResultSet(ts);
			
			if (req.from != null)
				ind.id = req.from.getId();

			WorkResult res = new WorkResult(true, ind, req.from, req.jobId);
			getSender().tell(res, getSelf());
		}
		catch(Exception e) {
			if (req.from != null)
				log.debug("failed to calc timesurface for feature %s", req.from.getId());
			else 
				log.debug("failed to calc timesurface for location %s", req.options.from);
			
			WorkResult res = new WorkResult(false, null, req.from, req.jobId);
			getSender().tell(res, getSelf());
		}
	}
	
	/** Perform profile routing 
	 * @throws Exception */
	private void onMsgOneToManyProfileRequest(OneToManyProfileRequest message) throws Exception {
		// check/get the right graph and sample set, or fail if we can't
		if (!checkGraph(message)) {
			log.error("unable to get graph %s", message.graphId);
			getSender().tell(new ResultFailed(), getSelf());
			return;
		}

		// is this an isochrone request?
		boolean isIsoRequest = message.destinationPointsetId == null;

		RepeatedRaptorProfileRouter rtr;
		try {
			// if this is an isochrone request the sampleset will be a regular grid
			rtr = new RepeatedRaptorProfileRouter(this.router.graph, message.options, sampleSet);
		} catch (Exception e) {
			if (message.from != null)
				log.debug("failed to calc timesurface for feature %s", message.from.getId());
			else 
				log.debug("failed to calc timesurface for location %s, %s", message.options.fromLat, message.options.fromLon);
			
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			getSender().tell(new WorkResult(false, null, null, null, message.from, message.jobId), getSelf());
			return;
		}
		
		try {
			rtr.route();			
			ResultSet.RangeSet res = rtr.makeResults(message.includeTimes, !isIsoRequest, isIsoRequest);
			
			if (message.from != null)
				res.max.id = res.avg.id = res.min.id = message.from.getId();
			
			// recall that min != worst
			WorkResult result1 = new WorkResult(true, res.min, res.avg, res.max, message.from, message.jobId);

			getSender().tell(result1, getSelf());
		} catch (Exception e) {
			if (message.from != null)
				log.debug("failed to calc timesurface for feature %s", message.from.getId());
			else 
				log.debug("failed to calc timesurface for location %s, %s", message.options.fromLat, message.options.fromLon);
			e.printStackTrace();
			// we use the version with three nulls to imply that it was a profile request
			WorkResult res = new WorkResult(false, null, null, null, message.from, message.jobId);
			getSender().tell(res, getSelf());
		}
		finally {
			//rtr.cleanup();
		}
	}
}