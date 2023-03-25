package com.alteirac.snowpipe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.time.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TinyServer {
    static Snowsdk s=new Snowsdk();
    
    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8100), 0);
        server.createContext("/test", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            InputStreamReader isr = new InputStreamReader(t.getRequestBody(), "utf-8");
            BufferedReader br = new BufferedReader(isr);
            int b;
            StringBuilder buf = new StringBuilder();
            while ((b = br.read()) != -1) {
                buf.append((char) b);
            }
            br.close();
            isr.close();
            Map<String, Object> row = new HashMap<>();
            JSONParser parser = new JSONParser();  
            JSONObject json;
            try {
                json = (JSONObject) parser.parse(buf.toString());
                row.put("TS", LocalDate.now());
                row.put("VALUE", json);
                s.sendMessage(row);
                String response =json+ " SENT !";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

            } catch (ParseException e) {
                String response ="SOMETHING GOES WRONG";
                t.sendResponseHeaders(500, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                e.printStackTrace();
            }  
        }
    }

}
