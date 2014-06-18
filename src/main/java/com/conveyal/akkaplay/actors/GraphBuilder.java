package com.conveyal.akkaplay.actors;

import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

import com.conveyal.akkaplay.message.BuildGraph;

import akka.actor.UntypedActor;

public class GraphBuilder extends UntypedActor {

	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof BuildGraph){
			BuildGraph bg = (BuildGraph)msg;
			
			URL gtfs_url = new URL("https://s3.amazonaws.com"+bg.gtfs_path);
			System.out.println( gtfs_url );
			URL osm_url = new URL("https://s3.amazonaws.com"+bg.osm_path);
			
			System.out.println( "getting gtfs" );
			ReadableByteChannel rbc = Channels.newChannel(gtfs_url.openStream());
			FileOutputStream fos = new FileOutputStream(UUID.randomUUID().toString()+".zip");
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			
			System.out.println( "getting osm" );
			rbc = Channels.newChannel(osm_url.openStream());
			fos = new FileOutputStream(UUID.randomUUID().toString()+".osm.pbf");
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			
			System.out.println( "start building graph gtfs:"+bg.gtfs_path+" osm:"+bg.osm_path );
		}
	}

}
