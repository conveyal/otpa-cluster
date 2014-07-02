package com.conveyal.otpac;

import com.conveyal.otpac.message.WorkResult;

public interface JobItemCallback {

	void onWorkResult(WorkResult res);

}
