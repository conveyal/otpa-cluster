package com.conveyal.otpac.handlers;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.JobItemCallback;
import com.conveyal.otpac.JobResultsApplication;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.WorkResult;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class FindHandler extends HttpHandler{

	private ActorRef executive;
	private JobResultsApplication statusServer;

	public FindHandler(ActorRef executive, JobResultsApplication statusServer) {
		this.executive = executive;
		this.statusServer = statusServer;
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
			JobSpec js = new JobSpec(bucket, fromPtsLoc, toPtsLoc, dateStr,
					timeStr, timezoneStr, mode, null);
			
			js.setCallback( new JobItemCallback(){

				@Override
				public void onWorkResult(WorkResult res) {
					try {
						statusServer.onWorkResult( res );
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			
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

}
