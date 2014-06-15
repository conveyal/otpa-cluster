package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class StartPrimeSearchHandler implements HttpHandler {
    private ActorRef taskMaster;

	public StartPrimeSearchHandler(ActorRef taskMaster) {
    	this.taskMaster = taskMaster;
	}

	public void handle(HttpExchange t) throws IOException {
        InputStream is = t.getRequestBody();
        
        String resp;
        
        String[] strJobParams = t.getRequestURI().getPath().split("/");
        if( strJobParams.length<2 ){
        	respond(t,404,"query format: /method/params");
        }
        String method = strJobParams[1];
        if(method.equals("find")){
        	if(strJobParams.length!=4){
        		respond(t,404,"query format: /find/fromnum/tonum");
        	}

        	long start = Long.parseLong( strJobParams[2] );
	        long end = Long.parseLong( strJobParams[3] );
	        
	        resp = "find primes from "+start+" to "+end;
	        
	        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
	        Future<Object> future = Patterns.ask(taskMaster, new JobSpec(start,end), timeout);
	        try {
				JobId result = (JobId) Await.result(future, timeout.duration());
				respond(t,200,"jobId:"+result.jobId);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				respond(t,500,"something went wrong");
			}
	        
        } else {
        	respond(t,404,"methods: ['find']");
        }
    }

	private void respond(HttpExchange t, int code, String resp) throws IOException {
		t.sendResponseHeaders(code, resp.length());
        OutputStream os = t.getResponseBody();
        os.write(resp.getBytes());
        os.close();
	}
}