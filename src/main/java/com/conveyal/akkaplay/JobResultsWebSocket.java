package com.conveyal.akkaplay;

import java.util.logging.Logger;

import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.websockets.DefaultWebSocket;
import org.glassfish.grizzly.websockets.ProtocolHandler;
import org.glassfish.grizzly.websockets.WebSocket;
import org.glassfish.grizzly.websockets.WebSocketListener;

public class JobResultsWebSocket extends DefaultWebSocket {
	private static final Logger logger = Grizzly.logger(JobResultsWebSocket.class);
	
	int jobId;

	public JobResultsWebSocket(int jobId, ProtocolHandler protocolHandler, HttpRequestPacket request,
			WebSocketListener... listeners) {
		super(protocolHandler, request, listeners);
		
		this.jobId = jobId;
	}
}
