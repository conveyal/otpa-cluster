package com.conveyal.akkaplay;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class StartPrimeSearchHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        InputStream is = t.getRequestBody();
        
        String[] strJobParams = t.getRequestURI().getPath().split("/");
        System.out.println( strJobParams[1]+" "+strJobParams[2] );
        long start = Long.parseLong( strJobParams[1] );
        long end = Long.parseLong( strJobParams[2] );
        System.out.println( start+" "+end );
        
        String response = "This is the response";
        t.sendResponseHeaders(200, response.length());
        OutputStream os = t.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}