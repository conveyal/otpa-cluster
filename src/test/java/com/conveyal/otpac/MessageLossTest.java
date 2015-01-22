package com.conveyal.otpac;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.core.TraverseModeSet;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.WorkerManager;
import com.conveyal.otpac.message.AddWorkerManager;
import com.conveyal.otpac.message.AssignExecutive;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;
import com.conveyal.otpac.message.WorkResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Tests of resiliency.
 * @author mattwigway
 *
 */
public class MessageLossTest {
	
	/**
	 * Test if a job completes even though the transport is unreliable.
	 */
	@Test
	public void testUnreliableTransport () throws Exception {
		Config cfg = ConfigFactory.load();
		ActorSystem sys = ActorSystem.create("unreliable-transport", cfg);
		
		ActorRef exec = sys.actorOf(Props.create(Executive.class, true, null, null));
		sys.actorOf(Props.create(UnreliableWorkerManager.class, exec, null, true, null, null));
		
		Thread.sleep(10 * 1000);
		
		RoutingRequest rr = new PrototypeAnalystRequest();		
		DateTime t = new DateTime(2014, 6, 9, 8, 5, DateTimeZone.forID("America/Chicago"));
		rr.dateTime = t.getMillis() / 1000;
		rr.setModes(new TraverseModeSet("TRANSIT"));
		
		JobSpec js = new JobSpec("austin", "austin.csv", "austin.csv", rr);
		
		Timeout timeout = new Timeout(Duration.create(10, "seconds"));
		Future<Object> f = Patterns.ask(exec, js, timeout);
		int jobId = ((JobId) Await.result(f, timeout.duration())).jobId;
				
		JobStatus oldStat = null;
		
		while (true) {
			f = Patterns.ask(exec, new JobStatusQuery(jobId), timeout);
			JobStatus stat = (JobStatus) Await.result(f, timeout.duration());
			
			if (oldStat != null && stat.complete == oldStat.complete) {
				// it hasn't changed in 30 seconds, probably done or stuck
				assertEquals(stat.total, stat.complete);
				break;
			}
			
			oldStat = stat;
			
			Thread.sleep(60 * 1000);
		}
	}
	
	

	/**
	 * This is a "special" workermanager that drops every 10th request.
	 */
	private static class UnreliableWorkerManager extends WorkerManager {
		public UnreliableWorkerManager(ActorRef executive, Integer nWorkers, Boolean workOffline,
				String graphsBucket, String pointsetsBucket) {
			super(executive, nWorkers, workOffline, graphsBucket, pointsetsBucket);
		}

		private static int rcvCount = 0;
		
		@Override
		public void onReceive(Object message) throws Exception {
			// for now we are assuming that the connection succeeds initially, but the whole system falls apart
			// if AssignExecutive or ActorIdentity get lost at the start.
			// We also assume that local message passing of workresults always succeeds
			if (rcvCount++ % 10 == 0 && !(message instanceof AssignExecutive || message instanceof ActorIdentity || message instanceof WorkResult))
				// drop it on the floor
				return;
			
			super.onReceive(message);
		}
	}	
}
