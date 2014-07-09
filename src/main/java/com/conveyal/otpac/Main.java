package com.conveyal.otpac;

import java.io.IOException;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.websockets.WebSocketAddOn;
import org.glassfish.grizzly.websockets.WebSocketEngine;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.conveyal.otpac.handlers.AddWorkerHandler;
import com.conveyal.otpac.handlers.FindHandler;
import com.conveyal.otpac.handlers.GetJobResultHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Main {

	public static void main(String[] args) throws IOException {		
		Config config;
		if(args.length > 0){
			String hostname = args[0];
			System.out.println( hostname );
			config = ConfigFactory.parseString("akka.remote.netty.tcp.hostname=\""+hostname+"\"")
		    .withFallback(ConfigFactory.load());
		} else {
			config = ConfigFactory.load();
		}
		String hostname = config.getString("akka.remote.netty.tcp.hostname");
		int port = config.getInt("akka.remote.netty.tcp.port");
		System.out.println("running on " + hostname + ":" + port);
		String role = config.getString("role");
		System.out.println("role: " + role);

		ActorSystem system = ActorSystem.create("MySystem", config);

		if (role.equals("taskmaster")) {
			// start the executive actor
			System.out.println("setting up master");
			ActorRef executive = system.actorOf(Props.create(Executive.class));

			HttpServer server = HttpServer.createSimpleServer("static");

			server.getListener("grizzly").registerAddOn(new WebSocketAddOn());

			// initialize websocket chat application
			JobResultsApplication chatApplication = new JobResultsApplication();

			// register the application
			WebSocketEngine.getEngine().register("/grizzly-websockets-chat", "/chat/*", chatApplication);
			
			server.getListener("grizzly").getFileCache().setEnabled(false);
			
			// set up webapp endpoints
			ServerConfiguration svCfg = server.getServerConfiguration();
			svCfg.addHttpHandler(new AddWorkerHandler(executive, system), "/addworker");
			svCfg.addHttpHandler(new GetJobResultHandler(executive), "/getstatus");
			svCfg.addHttpHandler(new FindHandler(executive, chatApplication), "/find");

			server.start();

		} else {
			ActorRef manager = system.actorOf(Props.create(WorkerManager.class), "manager");
			System.out.println("spinning up actor with path: " + manager.path());
		}

	}
}
