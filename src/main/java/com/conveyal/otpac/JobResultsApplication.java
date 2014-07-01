/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2010-2013 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

package com.conveyal.otpac;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.websockets.DataFrame;
import org.glassfish.grizzly.websockets.ProtocolHandler;
import org.glassfish.grizzly.websockets.WebSocket;
import org.glassfish.grizzly.websockets.WebSocketApplication;
import org.glassfish.grizzly.websockets.WebSocketListener;

import com.conveyal.otpac.message.WorkResult;

public class JobResultsApplication extends WebSocketApplication {
    private static final Logger logger = Grizzly.logger(JobResultsApplication.class);
    
    Map<Integer,Set<JobResultsWebSocket>> clients;
    
    JobResultsApplication(){
    	clients = new HashMap<Integer,Set<JobResultsWebSocket>>();
    }

    @Override
    public WebSocket createSocket(ProtocolHandler handler,
                                  HttpRequestPacket request,
                                  WebSocketListener... listeners) {
    	String uri = request.getRequestURI();
    	String[] uri_parts = uri.split("/");
    	String jobIdStr = uri_parts[uri_parts.length-1];
    	int jobId = Integer.parseInt( jobIdStr );
    	
        return new JobResultsWebSocket(jobId, handler, request, listeners);
    }
    
    public void onWorkResult(WorkResult wr){
    	Set<JobResultsWebSocket> jobClients = clients.get(wr.jobId);
    	if(jobClients!=null){
    		for( JobResultsWebSocket sock : jobClients ){
    			sock.send( wr.toJsonString() );
    		}
    	}
    }

    @Override
    public void onMessage(WebSocket websocket, String data) {
    	
    	websocket.send( data );
    	
    }

    @Override
    public void onConnect(WebSocket socket) {
    	if( socket instanceof JobResultsWebSocket ) {
    		JobResultsWebSocket rslt = (JobResultsWebSocket)socket;
    		System.out.println( "socket connected for job: "+rslt.jobId );
    		
    		if( !clients.containsKey(rslt.jobId) ){
    			clients.put(rslt.jobId, new HashSet<JobResultsWebSocket>());
    		}
    		
    		clients.get(rslt.jobId).add(rslt);
    	}
    }

    @Override
    public void onClose(WebSocket websocket, DataFrame frame) {
    	if(!(websocket instanceof JobResultsWebSocket)){
    		return;
    	}
    	
    	JobResultsWebSocket rslt = (JobResultsWebSocket)websocket;
    	Set<JobResultsWebSocket> jobClients = clients.get(rslt.jobId);
    	jobClients.remove(rslt);
    	if( jobClients.size()==0 ){
    		clients.remove(rslt.jobId);
    	}
    }

}

