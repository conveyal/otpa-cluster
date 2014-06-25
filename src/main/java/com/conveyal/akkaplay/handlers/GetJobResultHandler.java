package com.conveyal.akkaplay.handlers;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.akkaplay.Histogram;
import com.conveyal.akkaplay.message.JobResult;
import com.conveyal.akkaplay.message.JobResultQuery;
import com.conveyal.akkaplay.message.WorkResult;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class GetJobResultHandler extends HttpHandler{

	private ActorRef executive;

	public GetJobResultHandler(ActorRef executive) {
		this.executive = executive;
	}

	@Override
	public void service(Request request, Response response) throws Exception {
		String jobIdStr = request.getParameter("jobid");
		String workIndexStr = request.getParameter("i");

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

			if(workIndexStr != null){
				int workItemIndex = Integer.parseInt(workIndexStr);
				WorkResult wr = result.res.get(workItemIndex);
				
				StringBuilder sb = new StringBuilder();
				sb.append( "WorkResult\n" );
				sb.append( wr.point+"\n" );
				for(Histogram hist : wr.histograms){
					sb.append( hist.name+" " );
					sb.append("[");
					for(int i=0; i<hist.bins.length; i++){
						sb.append( hist.bins[i]+"," );
					}
					sb.append("]\n");
				}
				response.getWriter().write( sb.toString() );
				return;
			} else {
				response.getWriter().write( "result.size:"+result.res.size() );
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
			response.setStatus( 500);
			response.getWriter().write("something went wrong");
		}
	}

}
