package com.conveyal.otpac.handlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.util.DateUtils;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.JobResultsApplication;
import com.conveyal.otpac.PrototypeAnalystRequest;
import com.conveyal.otpac.actors.JobItemActor;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.WorkResult;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class FindHandler extends HttpHandler{

	private ActorRef executive;
	private JobResultsApplication statusServer;
	private ActorSystem system;

	/**
	 * Create a FindHandler for the given executive. An ActorSystem is needed for the registration of callbacks.
	 */
	public FindHandler(ActorRef executive, JobResultsApplication statusServer, ActorSystem system) {
		this.executive = executive;
		this.statusServer = statusServer;
		this.system = system;
	}

	@Override
	public void service(Request request, Response response) throws Exception {

		String bucket = request.getParameter("graphid");
		String fromPtsLoc = request.getParameter("from");
		String toPtsLoc = request.getParameter("to");
		String dateStr = request.getParameter("date");
		String timeStr = request.getParameter("time");
		String timezoneStr = request.getParameter("tz");
		String mode = request.getParameter("mode");
		
		if (mode == null)
			mode = "TRANSIT";

		if (bucket == null) {
			response.setStatus(400);
			response.getWriter().write("'bucket' is not optional");
			return;
		}
		if (fromPtsLoc == null) {
			response.setStatus(400);
			response.getWriter().write("'from' is not optional");
			return;
		}
		if (toPtsLoc == null) {
			response.setStatus(400);
			response.getWriter().write("'to' is not optional");
			return;
		}
		if (dateStr == null) {
			response.setStatus(400);
			response.getWriter().write("'to' is not optional");
			return;
		}
		if (timeStr == null) {
			response.setStatus(400);
			response.getWriter().write("'time' is not optional");
			return;
		}
		if (timezoneStr == null) {
			response.setStatus(400);
			response.getWriter().write("'tz' is not optional");
			return;
		}

		try {
			//  build a routing request
			RoutingRequest rr = new PrototypeAnalystRequest();
			
			TimeZone tz = TimeZone.getTimeZone(timezoneStr);
			Date date = DateUtils.toDate(dateStr, timeStr, tz);
			rr.dateTime = date.getTime() / 1000;
			
			rr.modes.clear();
			switch(mode) {
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
			
			
			JobSpec js = new JobSpec(bucket, fromPtsLoc, toPtsLoc, rr);
						
			js.setCallback(system.actorOf(Props.create(StatusServerJobItemActor.class, statusServer)));
			
			
			
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> future = Patterns.ask(executive, js, timeout);
			JobId result = (JobId) Await.result(future, timeout.duration());
			
			response.getWriter().write( "{\"jobId\":" + result.jobId+"}" );
		} catch (TimeoutException e) {
			response.setStatus( 500);
			response.getWriter().write("request timed out");
		} catch (Exception e) {
			e.printStackTrace();
			response.setStatus( 500);
			response.getWriter().write("something went wrong");
		}
	}
	
	/**
	 * Notify the status server when there is a workresult.
	 *  
	 * @author mattwigway
	 */
	public static class StatusServerJobItemActor extends JobItemActor {
		private JobResultsApplication application;
		
		public StatusServerJobItemActor(JobResultsApplication application) {
			this.application = application;
		}
		
		@Override
		public void onWorkResult(WorkResult wr) {
			try {
				application.onWorkResult(wr);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}

