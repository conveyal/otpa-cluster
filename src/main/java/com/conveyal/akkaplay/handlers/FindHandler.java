package com.conveyal.akkaplay.handlers;

import java.util.concurrent.TimeoutException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.akkaplay.message.JobId;
import com.conveyal.akkaplay.message.JobSpec;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class FindHandler extends HttpHandler{

	private ActorRef executive;

	public FindHandler(ActorRef executive) {
		this.executive = executive;
	}

	@Override
	public void service(Request request, Response response) throws Exception {

		String bucket = request.getParameter("graphid");
		String fromPtsLoc = request.getParameter("from");
		String toPtsLoc = request.getParameter("to");
		String dateStr = request.getParameter("date");
		String timeStr = request.getParameter("time");
		String timezoneStr = request.getParameter("tz");

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
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> future = Patterns.ask(executive, new JobSpec(bucket, fromPtsLoc, toPtsLoc, dateStr,
					timeStr, timezoneStr), timeout);
			JobId result = (JobId) Await.result(future, timeout.duration());
			
			response.setStatus( 200, "jobId:" + result.jobId);
			response.getWriter().write( "jobId:" + result.jobId );
		} catch (TimeoutException e) {
			response.setStatus( 500);
			response.getWriter().write("request timed out");
		} catch (Exception e) {
			e.printStackTrace();
			response.setStatus( 500);
			response.getWriter().write("something went wrong");
		}
	}

}
