package com.conveyal.otpac;

import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.websockets.DefaultWebSocket;
import org.glassfish.grizzly.websockets.ProtocolHandler;
import org.glassfish.grizzly.websockets.WebSocketListener;

public class JobResultsWebSocket extends DefaultWebSocket {
	
	int jobId;

	public JobResultsWebSocket(int jobId, ProtocolHandler protocolHandler, HttpRequestPacket request,
			WebSocketListener... listeners) {
		super(protocolHandler, request, listeners);
		
		this.jobId = jobId;
	}
}
