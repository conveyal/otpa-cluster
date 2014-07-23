package com.conveyal.otpac.handlers;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import com.conveyal.otpac.message.AddWorkerManager;

import akka.actor.ActorRef;

public class CancelHandler extends HttpHandler {

	private ActorRef executive;

	public CancelHandler(ActorRef executive) {
		this.executive = executive;
	}

	@Override
	public void service(Request request, Response response) throws Exception {
		String jobid = request.getParameterMap().get("jobid")[0];

		//executive.tell(new AddWorkerManager("akka.tcp://" + path), ActorRef.noSender());
		
		response.getWriter().write( "{\"jobid\":"+jobid+"}" );
	}

}
