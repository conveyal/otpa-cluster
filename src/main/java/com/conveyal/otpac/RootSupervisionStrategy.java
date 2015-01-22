package com.conveyal.otpac;

import scala.concurrent.duration.Duration;
import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.SupervisorStrategyConfigurator;
import akka.japi.Function;

/**
 * A supervision strategy that resumes on all errors, because we never want to restart the executive or the workermanager.
 * @author mattwigway
 *
 */
public class RootSupervisionStrategy implements SupervisorStrategyConfigurator {

	private static final SupervisorStrategy supervisorStrategy =
			new OneForOneStrategy(10, Duration.create(1, "minute"),
					new Function<Throwable, Directive> () {

						@Override
						public Directive apply(Throwable arg0) throws Exception {
							return SupervisorStrategy.resume();
						}
				
			});
	
	@Override
	public SupervisorStrategy create() {
		return supervisorStrategy;
	}

}
