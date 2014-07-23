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
import com.conveyal.otpac.handlers.CancelHandler;
import com.conveyal.otpac.handlers.FindHandler;
import com.conveyal.otpac.handlers.GetJobResultHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Main {

	public static void main(String[] args) throws IOException, ParseException {
		// set up command line parser
		Options options = new Options();
		options.addOption( "h", true, "hostname");
		options.addOption( "p", true, "port" );
		
		// parse command line options
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		
		// get hostname for both akka-remoting and http interface
		Config config = ConfigFactory.load();
		String hostname;
		if(cmd.hasOption('h')){
			hostname = cmd.getOptionValue('h');
		} else {
			hostname = config.getString("akka.remote.netty.tcp.hostname");
		}
		
		// get port for http interface
		int webPort;
		if(cmd.hasOption('p')){
			webPort = Integer.parseInt(cmd.getOptionValue('p'));
		} else {
			webPort = 8080;
		}
		
		// print some server info
		int akkaPort = config.getInt("akka.remote.netty.tcp.port");
		System.out.println("running on " + hostname + ":" + akkaPort);
		String role = config.getString("role");
		System.out.println("role: " + role);

		ActorSystem system = ActorSystem.create("MySystem", config);

		if (role.equals("taskmaster")) {
			// start the executive actor
			System.out.println("setting up master");
			ActorRef executive = system.actorOf(Props.create(Executive.class));

			// start the http server
			HttpServer server = HttpServer.createSimpleServer("static", hostname, webPort);
			server.getListener("grizzly").registerAddOn(new WebSocketAddOn());

			// initialize websocket chat application
			JobResultsApplication chatApplication = new JobResultsApplication();

			// register the application
			WebSocketEngine.getEngine().register("/grizzly-websockets-chat", "/chat/*", chatApplication);
			
			// turn off file caching. for development only. remove in production.
			server.getListener("grizzly").getFileCache().setEnabled(false);
			
			// set up webapp endpoints
			ServerConfiguration svCfg = server.getServerConfiguration();
			svCfg.addHttpHandler(new AddWorkerHandler(executive), "/addworker");
			svCfg.addHttpHandler(new GetJobResultHandler(executive), "/getstatus");
			svCfg.addHttpHandler(new FindHandler(executive, chatApplication), "/find");
			svCfg.addHttpHandler(new CancelHandler(executive), "/cancel");

			server.start();

		} else {
			// start up workermanager
			ActorRef manager = system.actorOf(Props.create(WorkerManager.class), "manager");
			System.out.println("spinning up actor with path: " + manager.path());
		}

	}
}
