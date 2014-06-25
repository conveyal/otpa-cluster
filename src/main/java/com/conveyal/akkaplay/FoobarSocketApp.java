package com.conveyal.akkaplay;

import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.websockets.DefaultWebSocket;
import org.glassfish.grizzly.websockets.ProtocolHandler;
import org.glassfish.grizzly.websockets.WebSocket;
import org.glassfish.grizzly.websockets.WebSocketApplication;
import org.glassfish.grizzly.websockets.WebSocketListener;

public class FoobarSocketApp extends WebSocketApplication {

	@Override
    public WebSocket createSocket(final ProtocolHandler handler, 
            final HttpRequestPacket requestPacket,
            final WebSocketListener... listeners) {
		System.out.println( "something is happening" );
		 
		return super.createSocket(handler, requestPacket, listeners);

}

}
