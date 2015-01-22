package com.conveyal.otpac;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.core.TraverseModeSet;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.util.Timeout;

import com.conveyal.otpac.actors.Executive;
import com.conveyal.otpac.actors.SPTWorker;
import com.conveyal.otpac.actors.WorkerManager;
import com.conveyal.otpac.actors.WorkerManager.Status;
import com.conveyal.otpac.message.JobId;
import com.conveyal.otpac.message.JobSpec;
import com.conveyal.otpac.message.JobStatus;
import com.conveyal.otpac.message.JobStatusQuery;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class CrashingWorkerManagerTest {
	/**
	 * Test if a job completes even though the workermanager is restarted partway through.
	 */
	@Test
	public void testCrashingWorkerManager () throws Exception {
		Config cfg = ConfigFactory.load();
		ActorSystem sys = ActorSystem.create("unreliable-transport", cfg);
		
		ActorRef exec = sys.actorOf(Props.create(Executive.class, true, null, null));
		ActorRef wm = sys.actorOf(Props.create(CrashingWorkerManager.class, exec, null, true, null, null));
		
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
		
		boolean notYetCrashed = true;
		
		while (true) {
			f = Patterns.ask(exec, new JobStatusQuery(jobId), timeout);
			JobStatus stat = (JobStatus) Await.result(f, timeout.duration());
			
			System.out.println("##### Status: " + stat.complete + " / " + stat.total);
			
			// crash/restart the worker manager
			if (stat.complete > 2000 && notYetCrashed) {
				wm.tell(new Crash(), ActorRef.noSender());
				notYetCrashed = false;
			}
			
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
	 * This is a special workermanager that crashes one of its workers when sent the "crash" message.
	 */
	private static class CrashingWorkerManager extends WorkerManager {
		public CrashingWorkerManager(ActorRef executive, Integer nWorkers,
				Boolean workOffline, String graphsBucket, String pointsetsBucket) {
			super(executive, nWorkers, workOffline, graphsBucket, pointsetsBucket);
		}

		@Override
		protected void createAndRouteWorkers() {
			ArrayList<Routee> routees = new ArrayList<Routee>();
			
			for (int i = 0; i < 4; i++) {
				ActorRef worker = getContext().actorOf(Props.create(CrashingSPTWorker.class, graphBuilder), "worker-" + i);
				routees.add(new ActorRefRoutee(worker));
				workers.add(worker);
			}
			
			router = new Router(new RoundRobinRoutingLogic(), routees);

			System.out.println("worker-manager: starting 4 workers");
			status = Status.READY;

		}
		
		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof Crash)
				router.route(message, getSelf());
			
			else
				super.onReceive(message);
		}
	}
	
	/**
	 * An SPTWorker that crashes when sent the crash message.
	 */
	private static class CrashingSPTWorker extends SPTWorker {
		public CrashingSPTWorker(ActorRef graphBuilder) {
			super(graphBuilder);
		}

		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof Crash)
				throw new Exception();
			
			super.onReceive(message);
		}
	}
	
	/**	Tell a CrashingWorkerManager to crash its workers */
	private static class Crash { /* do nothing */ }
}
