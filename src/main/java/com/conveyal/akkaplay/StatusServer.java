package com.conveyal.akkaplay;

import java.net.InetSocketAddress;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import com.conveyal.akkaplay.message.WorkResult;

public class StatusServer extends WebSocketServer {

	public StatusServer(InetSocketAddress address) {
		super(address);
	}

	@Override
	public void onOpen(WebSocket conn, ClientHandshake handshake) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onClose(WebSocket conn, int code, String reason, boolean remote) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessage(WebSocket conn, String message) {
		System.out.println( message );
		conn.send(message.replace('r', 'w'));
	}

	@Override
	public void onError(WebSocket conn, Exception ex) {
		// TODO Auto-generated method stub
		
	}

	public void onWorkResult(WorkResult wr) {
		for( WebSocket ws : this.connections() ){
			ws.send( wr.toString() );
		}
	}

}
