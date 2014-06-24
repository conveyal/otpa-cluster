package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.akkaplay.message.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class StartPrimeSearchHandler implements HttpHandler {
	private ActorRef executive;
	private ActorSystem system;

	public StartPrimeSearchHandler(ActorRef executive, ActorSystem system) {
		this.executive = executive;
		this.system = system;
	}

	private Map<String, String> parseQS(String qs) {
		HashMap<String, String> ret = new HashMap<String, String>();

		String[] pairs = qs.split("&");
		for (String pair : pairs) {
			String[] halves = pair.split("=");
			ret.put(halves[0], halves[1]);
		}

		return ret;
	}

	public void handle(HttpExchange t) throws IOException {
		URI uri = t.getRequestURI();
		String[] strJobParams = uri.getPath().split("/");
		if (strJobParams.length < 2) {
			respond(t, 404, "query format: /method/params");
		}
		String method = strJobParams[1];
		if (method.equals("find")) {
			if (strJobParams.length != 2) {
				respond(t, 400, "query format: /find?graphid=blah");
			}

			Map<String, String> params = parseQS(uri.getQuery()); // TODO more
																	// secure
																	// query
																	// string
																	// parsing
			String bucket = params.get("graphid");
			String fromPtsLoc = params.get("from");
			String toPtsLoc = params.get("to");
			String dateStr = params.get("date");
			String timeStr = params.get("time");
			String timezoneStr = params.get("tz");

			if (bucket == null) {
				respond(t, 400, "'bucket' is not optional");
			}
			if (fromPtsLoc == null) {
				respond(t, 400, "'from' is not optional");
			}
			if (toPtsLoc == null) {
				respond(t, 400, "'to' is not optional");
			}
			if (dateStr == null) {
				respond(t, 400, "'date' is not optional");
			}
			if (timeStr == null) {
				respond(t, 400, "'time' is not optional");
			}
			if (timezoneStr == null) {
				respond(t, 400, "'tz' is not optional");
			}

			try {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(executive, new JobSpec(bucket, fromPtsLoc, toPtsLoc, dateStr,
						timeStr, timezoneStr), timeout);
				JobId result = (JobId) Await.result(future, timeout.duration());
				respond(t, 200, "jobId:" + result.jobId);
			} catch (TimeoutException e) {
				respond(t, 500, "request timed out");
			} catch (Exception e) {
				e.printStackTrace();
				respond(t, 500, "something went wrong");
			}
		} else if (method.equals("jobresult")) {
			if (strJobParams.length < 3) {
				respond(t, 400, "query format: /jobresult/jobid/[workitem]");
			}

			int jobId = Integer.parseInt(strJobParams[2]);

			try {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(executive, new JobResultQuery(jobId), timeout);
				JobResult result = (JobResult) Await.result(future, timeout.duration());

				if(strJobParams.length>3){
					int workItemIndex = Integer.parseInt(strJobParams[3]);
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
					respond(t, 200, sb.toString());
				} else {
					respond(t, 200, "result.size:"+result.res.size());
				}
			} catch (Exception e) {
				e.printStackTrace();
				respond(t, 500, "something went wrong");
			}
		} else if (method.equals("jobstatus")) {
			if (strJobParams.length != 2) {
				respond(t, 400, "query format: /jobstatus");
			}

			try {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(executive, new JobStatusQuery(), timeout);
				ArrayList<JobStatus> result = (ArrayList<JobStatus>) Await.result(future, timeout.duration());

				StringBuilder bld = new StringBuilder();
				for (JobStatus js : result) {
					bld.append(js.manager + ":" + js.curJobId + ":" + js.fractionComplete + "\n");
				}

				respond(t, 200, bld.toString());
			} catch (Exception e) {
				e.printStackTrace();
				respond(t, 500, "something went wrong");
			}
		} else if (method.equals("addworker")) {
			String path = t.getRequestURI().getPath().substring(11);

			ActorSelection remoteManager = system.actorSelection("akka.tcp://" + path);
			executive.tell(new AddManager(remoteManager), ActorRef.noSender());

			respond(t, 200, "'" + path + "' added to worker pool");
		} else {
			respond(t, 404, "methods: ['find','jobresult']");
		}
	}

	private void respond(HttpExchange t, int code, String resp) throws IOException {
		t.sendResponseHeaders(code, resp.length());
		OutputStream os = t.getResponseBody();
		os.write(resp.getBytes());
		os.close();
	}
}