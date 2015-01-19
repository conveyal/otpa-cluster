package com.conveyal.otpac.handlers;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.message.JobResult;
import com.conveyal.otpac.message.JobResultQuery;
import com.conveyal.otpac.message.WorkResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class GetJobResultHandler extends HttpHandler{

	private ActorRef executive;
	
	private ObjectMapper om;

	public GetJobResultHandler(ActorRef executive) {
		this.executive = executive;
		om = new ObjectMapper();
	}

	@Override
	public void service(Request request, Response response) throws Exception {
		String jobIdStr = request.getParameter("jobid");

		if( jobIdStr==null ){
			response.setStatus(400);
			response.getWriter().write("'jobid' is not optional");
			return;
		}
		
		int jobId = Integer.parseInt(jobIdStr);

		try {
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> future = Patterns.ask(executive, new JobResultQuery(jobId), timeout);
			JobResult result = (JobResult) Await.result(future, timeout.duration());

			response.setStatus(200);
			response.setContentType("application/json");
			response.getWriter().write(om.writeValueAsString(result));
		} catch (Exception e) {
			e.printStackTrace();
			response.setStatus( 500);
			response.getWriter().write("something went wrong");
		}
	}

}
