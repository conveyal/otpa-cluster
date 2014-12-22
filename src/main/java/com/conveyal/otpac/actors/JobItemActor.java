package com.conveyal.otpac.actors;

import com.conveyal.otpac.message.WorkResult;

import akka.actor.UntypedActor;

/**
 * Handle a work result. This used to be called JobItemCallback, but it needs to
 * be a full-fledged actor for remoting to work correctly.
 * 
 * @author mattwigway
 */
public abstract class JobItemActor extends UntypedActor {

	@Override
	public void onReceive(Object arg0) throws Exception {
		if (arg0 instanceof WorkResult)
			onWorkResult((WorkResult) arg0);
	}
	
	public abstract void onWorkResult (WorkResult wr);

}
