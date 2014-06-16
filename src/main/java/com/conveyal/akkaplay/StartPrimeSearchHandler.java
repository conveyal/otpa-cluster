package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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
    private ActorRef taskMaster;
	private ActorSystem system;

	public StartPrimeSearchHandler(ActorRef taskMaster, ActorSystem system) {
    	this.taskMaster = taskMaster;
    	this.system = system;
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
        		respond(t,400,"query format: /find/fromnum/tonum");
        	}

        	long start = Long.parseLong( strJobParams[2] );
	        long end = Long.parseLong( strJobParams[3] );
	        
	        resp = "find primes from "+start+" to "+end;
	        
	        try {
		        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		        Future<Object> future = Patterns.ask(taskMaster, new JobSpec(start,end), timeout);
				JobId result = (JobId) Await.result(future, timeout.duration());
				respond(t,200,"jobId:"+result.jobId);
	        } catch (TimeoutException e){
	        	respond(t,500,"request timed out");
			} catch (Exception e) {
				e.printStackTrace();
				respond(t,500,"something went wrong");
			}
        } else if( method.equals("jobresult") ){
        	if(strJobParams.length!=3){
        		respond(t,400,"query format: /jobresult/jobid");
        	}
        	
        	int jobId = Integer.parseInt( strJobParams[2] );
        	
	        try {
		        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
		        Future<Object> future = Patterns.ask(taskMaster, new JobResultQuery(jobId), timeout);
				JobResult result = (JobResult) Await.result(future, timeout.duration());
				
				StringBuilder bld = new StringBuilder();
				for(int i=0; i<result.res.size(); i++){
					WorkResult wr = result.res.get(i);
					bld.append( wr.num+":"+wr.isPrime+"\n" );
				}
				
				respond(t,200,bld.toString());
			} catch (Exception e) {
				e.printStackTrace();
				respond(t,500,"something went wrong");
			}
        } else if( method.equals("addworker") ){
        	String path = t.getRequestURI().getPath().substring(11);
        	
        	ActorSelection remoteTaskMaster = system.actorSelection("akka.tcp://"+path);
        	taskMaster.tell( new AddWorker( remoteTaskMaster ), ActorRef.noSender() );
        	
        	respond(t,200,"'"+path+"' added to worker pool" );
        } else {
        	respond(t,404,"methods: ['find','jobresult']");
        }
    }

	private void respond(HttpExchange t, int code, String resp) throws IOException {
		t.sendResponseHeaders(code, resp.length());
        OutputStream os = t.getResponseBody();
        os.write(resp.getBytes());
        os.close();
	}
}