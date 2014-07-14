package com.conveyal.otpac;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

	public static void main(String[] args) throws IOException, ParseException {
		Options options = new Options();
		options.addOption( "h", true, "hostname");
		options.addOption( "p", true, "port" );
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		
		Config config;
		if(cmd.hasOption('h')){
			String hostname = cmd.getOptionValue('h');
			System.out.println( hostname );
			config = ConfigFactory.parseString("akka.remote.netty.tcp.hostname=\""+hostname+"\"")
		    .withFallback(ConfigFactory.load());
		} else {
			config = ConfigFactory.load();
		}
		
		int webPort;
		if(cmd.hasOption('p')){
			webPort = Integer.parseInt(cmd.getOptionValue('p'));
		} else {
			webPort = 8080;
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

			HttpServer server = HttpServer.createSimpleServer("static", hostname, webPort);

			server.getListener("grizzly").registerAddOn(new WebSocketAddOn());

			// initialize websocket chat application
			JobResultsApplication chatApplication = new JobResultsApplication();

			// register the application
			WebSocketEngine.getEngine().register("/grizzly-websockets-chat", "/chat/*", chatApplication);
			
			server.getListener("grizzly").getFileCache().setEnabled(false);
			
			// set up webapp endpoints
			ServerConfiguration svCfg = server.getServerConfiguration();
			svCfg.addHttpHandler(new AddWorkerHandler(executive), "/addworker");
			svCfg.addHttpHandler(new GetJobResultHandler(executive), "/getstatus");
			svCfg.addHttpHandler(new FindHandler(executive, chatApplication), "/find");

			server.start();

		} else {
			ActorRef manager = system.actorOf(Props.create(WorkerManager.class), "manager");
			System.out.println("spinning up actor with path: " + manager.path());
		}

	}
}
