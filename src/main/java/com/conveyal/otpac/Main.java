package com.conveyal.otpac;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.websockets.WebSocketAddOn;
import org.glassfish.grizzly.websockets.WebSocketEngine;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.conveyal.otpac.handlers.AddWorkerHandler;
import com.conveyal.otpac.handlers.CancelHandler;
import com.conveyal.otpac.handlers.FindHandler;
import com.conveyal.otpac.handlers.GetJobResultHandler;
import com.conveyal.otpac.workers.ThreadWorkerFactory;
import com.conveyal.otpac.workers.WorkerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;

public class Main {

	public static void main(String[] args) throws IOException, ParseException {
		// set up command line parser
		Options options = new Options();
		options.addOption( "h", true, "hostname");
		options.addOption( "p", true, "port" );
		options.addOption("l", "local", false, "Should everything be run locally?");
		options.addOption("m", "machines", true, "number of machines to use.");
		options.addOption("w", "worker", true, "register as a worker for the given akka url.");
		
		
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

		if (!cmd.hasOption("worker")) {
			// start the executive actor
			System.out.println("setting up master");
			ActorRef executive = system.actorOf(Props.create(Executive.class), "executive");

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
			svCfg.addHttpHandler(new FindHandler(executive, chatApplication, system), "/find");
			svCfg.addHttpHandler(new CancelHandler(executive), "/cancel");
			
			WorkerFactory factory;
			
			if (cmd.hasOption("local"))
				// running everything locally; start the appropriate number of WorkerManagers
				factory = new ThreadWorkerFactory(system);
			else
				// TODO: spin up EC2 instances, etc.
				factory = new ThreadWorkerFactory(system);

			// start the appropriate number of workermanagers
			int number = Integer.parseInt(cmd.getOptionValue("machines", "1"));
			Collection<ActorRef> actors = factory.createWorkerManagers(number, executive);
			
			try {
				server.start();
				
				while (true) {
					Thread.sleep(72000000);
				}
			} catch (Exception e) {
			} finally {
				for (ActorRef actorRef : actors) {
					factory.terminateWorkerManager(actorRef, executive);
				}
			}

			
		} else {
			// start up workermanager and bind to the named executive
			ActorSelection executiveSelection = system.actorSelection(cmd.getOptionValue("worker"));
			
			// find the executive
			Timeout timeout = new Timeout(Duration.create(60, "seconds"));
			Future<Object> future = Patterns.ask(executiveSelection, new Identify("1"), timeout);
			
			WorkerFactory factory;
			ActorRef executive;
			
			try {
				ActorIdentity actorId = (ActorIdentity) Await.result( future, timeout.duration() );
				executive = actorId.getRef();
				
				// create a workermanager
				factory = new ThreadWorkerFactory(system);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			ActorRef mgr = factory.createWorkerManagers(1, executive).iterator().next();
			
			try {
				while(true) {
					Thread.sleep(7200000);
				}
			} catch (Exception e) {
			} finally {
				factory.terminateWorkerManager(mgr, executive);
			}
		}
	}
}
