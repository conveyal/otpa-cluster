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
	
	private Map<String,String> parseQS(String qs){
		HashMap<String,String> ret = new HashMap<String,String>();
		
		String[] pairs = qs.split("&");
		for(String pair : pairs){
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
			if (strJobParams.length != 3) {
				respond(t, 400, "query format: /find/{bucket}");
			}
			
			String bucket = strJobParams[2];

			try {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(executive, new JobSpec(bucket), timeout);
				JobId result = (JobId) Await.result(future, timeout.duration());
				respond(t, 200, "jobId:" + result.jobId);
			} catch (TimeoutException e) {
				respond(t, 500, "request timed out");
			} catch (Exception e) {
				e.printStackTrace();
				respond(t, 500, "something went wrong");
			}
		} else if (method.equals("jobresult")) {
			if (strJobParams.length != 3) {
				respond(t, 400, "query format: /jobresult/jobid");
			}

			int jobId = Integer.parseInt(strJobParams[2]);

			try {
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(executive, new JobResultQuery(jobId), timeout);
				JobResult result = (JobResult) Await.result(future, timeout.duration());

				StringBuilder bld = new StringBuilder();
				for (int i = 0; i < result.res.size(); i++) {
					WorkResult wr = result.res.get(i);
					bld.append(wr.num + ":" + wr.isPrime + "\n");
				}

				respond(t, 200, bld.toString());
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