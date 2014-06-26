package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.websockets.WebSocketAddOn;
import org.glassfish.grizzly.websockets.WebSocketApplication;
import org.glassfish.grizzly.websockets.WebSocketEngine;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.akkaplay.actors.Executive;
import com.conveyal.akkaplay.actors.Manager;
import com.conveyal.akkaplay.actors.SPTWorker;
import com.conveyal.akkaplay.handlers.AddWorkerHandler;
import com.conveyal.akkaplay.handlers.FindHandler;
import com.conveyal.akkaplay.handlers.GetJobResultHandler;
import com.conveyal.akkaplay.message.SetStatusServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;

public class Main {

	public static void main(String[] args) throws IOException {

		// AWSCredentials creds = new
		// ProfileCredentialsProvider().getCredentials(); //grab credentials
		// from "~.aws/credentials"
		// AmazonS3 s3 = new AmazonS3Client(creds);
		// ObjectListing ol = s3.listObjects("otpac");
		// for( S3ObjectSummary os : ol.getObjectSummaries() ){
		// System.out.println( os.getKey() );
		// }

		Config config = ConfigFactory.load();
		String hostname = config.getString("akka.remote.netty.tcp.hostname");
		int port = config.getInt("akka.remote.netty.tcp.port");
		System.out.println("running on " + hostname + ":" + port);
		String role = config.getString("role");
		System.out.println("role: " + role);

		ActorSystem system = ActorSystem.create("MySystem");

		if (role.equals("taskmaster")) {
			// start the executive actor
			System.out.println("setting up master");
			ActorRef executive = system.actorOf(Props.create(Executive.class));

			HttpServer server = HttpServer.createSimpleServer("static");
			ServerConfiguration svCfg = server.getServerConfiguration();
			svCfg.addHttpHandler(new AddWorkerHandler(executive, system), "/addworker");
			svCfg.addHttpHandler(new GetJobResultHandler(executive), "/getstatus");
			svCfg.addHttpHandler(new FindHandler(executive), "/find");

			server.getListener("grizzly").registerAddOn(new WebSocketAddOn());

			// initialize websocket chat application
			JobResultsApplication chatApplication = new JobResultsApplication();

			// link executive up with socket server
			executive.tell(new SetStatusServer(chatApplication), ActorRef.noSender());

			// register the application
			WebSocketEngine.getEngine().register("/grizzly-websockets-chat", "/chat/*", chatApplication);
			
			server.getListener("grizzly").getFileCache().setEnabled(false);

			server.start();

		} else {
			ActorRef manager = system.actorOf(Props.create(Manager.class), "manager");
			System.out.println("spinning up actor with path: " + manager.path());
		}

	}
}
