package com.conveyal.otpac.handlers;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import com.conveyal.otpac.message.AddManager;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;

public class AddWorkerHandler extends HttpHandler{

	private ActorRef executive;
	private ActorSystem system;

	public AddWorkerHandler(ActorRef executive, ActorSystem system) {
		this.executive = executive;
		this.system = system;
	}

	@Override
	public void service(Request request, Response response) throws Exception {
		String path = request.getParameterMap().get("path")[0];

		ActorSelection remoteManager = system.actorSelection("akka.tcp://" + path);
		executive.tell(new AddManager(remoteManager), ActorRef.noSender());

		response.getWriter().write( "'" + path + "' added to worker pool" );
	}

}
