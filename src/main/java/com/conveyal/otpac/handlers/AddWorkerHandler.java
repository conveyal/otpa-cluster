package com.conveyal.otpac.handlers;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import com.conveyal.otpac.message.AddManager;

import akka.actor.ActorRef;

public class AddWorkerHandler extends HttpHandler{

	private ActorRef executive;

	public AddWorkerHandler(ActorRef executive) {
		this.executive = executive;
	}

	@Override
	public void service(Request request, Response response) throws Exception {
		String path = request.getParameterMap().get("path")[0];

		executive.tell(new AddManager("akka.tcp://" + path), ActorRef.noSender());

		response.getWriter().write( "'" + path + "' added to worker pool" );
	}

}
