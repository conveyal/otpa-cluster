package com.conveyal.otpac.standalone;

import com.conveyal.otpac.message.WorkResult;

public interface JobItemCallback {

	void onWorkResult(WorkResult res);

}
