package com.conveyal.otpac;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.opentripplanner.graph_builder.GraphBuilderTask;
import org.opentripplanner.graph_builder.impl.GtfsGraphBuilderImpl;
import org.opentripplanner.graph_builder.impl.PruneFloatingIslands;
import org.opentripplanner.graph_builder.impl.TransitToStreetNetworkGraphBuilderImpl;
import org.opentripplanner.graph_builder.impl.osm.DefaultWayPropertySetSource;
import org.opentripplanner.graph_builder.impl.osm.OpenStreetMapGraphBuilderImpl;
import org.opentripplanner.graph_builder.model.GtfsBundle;
import org.opentripplanner.openstreetmap.impl.AnyFileBasedOpenStreetMapProviderImpl;
import org.opentripplanner.openstreetmap.services.OpenStreetMapProvider;

public class ClusterGraphBuilder {

	public static GraphBuilderTask createBuilder(File dir) {

		GraphBuilderTask graphBuilder = new GraphBuilderTask();
		List<File> gtfsFiles = new ArrayList<File>();
		List<File> osmFiles = new ArrayList<File>();
		/*
		 * For now this is adding files from all directories listed, rather than
		 * building multiple graphs.
		 */

		if (!dir.isDirectory() && dir.canRead()) {
			return null;
		}

		for (File file : recursiveListFiles(dir,null)) {
			switch (InputFileType.forFile(file)) {
			case GTFS:
				System.out.println("Found GTFS file " + file);
				gtfsFiles.add(file);
				break;
			case OSM:
				System.out.println("Found OSM file " + file);
				osmFiles.add(file);
				break;
			case OTHER:
				System.out.println("Skipping file ''" + file);
			default:
				break;
			}
		}

		boolean hasOSM = !(osmFiles.isEmpty());
		boolean hasGTFS = !(gtfsFiles.isEmpty());

		if (!(hasOSM || hasGTFS)) {
			System.out.println( "Found no input files from which to build a graph in "+ dir.toString() );
			return null;
		}

		if (hasOSM) {
			List<OpenStreetMapProvider> osmProviders = new ArrayList<OpenStreetMapProvider>();
			for (File osmFile : osmFiles) {
				OpenStreetMapProvider osmProvider = new AnyFileBasedOpenStreetMapProviderImpl(osmFile);
				osmProviders.add(osmProvider);
			}
			OpenStreetMapGraphBuilderImpl osmBuilder = new OpenStreetMapGraphBuilderImpl(osmProviders);
			DefaultWayPropertySetSource defaultWayPropertySetSource = new DefaultWayPropertySetSource();
			osmBuilder.setDefaultWayPropertySetSource(defaultWayPropertySetSource);
			osmBuilder.skipVisibility = false;
			graphBuilder.addGraphBuilder(osmBuilder);
			graphBuilder.addGraphBuilder(new PruneFloatingIslands());
		}
		if (hasGTFS) {
			List<GtfsBundle> gtfsBundles = new ArrayList<GtfsBundle>();
			for (File gtfsFile : gtfsFiles) {
				GtfsBundle gtfsBundle = new GtfsBundle(gtfsFile);
				gtfsBundle.setTransfersTxtDefinesStationPaths(false);
				gtfsBundle.linkStopsToParentStations = false;
				gtfsBundle.parentStationTransfers = false;
				gtfsBundles.add(gtfsBundle);
			}
			GtfsGraphBuilderImpl gtfsBuilder = new GtfsGraphBuilderImpl(gtfsBundles);
			graphBuilder.addGraphBuilder(gtfsBuilder);

			graphBuilder.addGraphBuilder(new TransitToStreetNetworkGraphBuilderImpl());

		}
		graphBuilder.serializeGraph = false;
		return graphBuilder;
	}

	private static ArrayList<File> recursiveListFiles(File dir, ArrayList<File> ret) {
		if(ret==null)
			ret = new ArrayList<File>();
		
		for( File file : dir.listFiles() ){
			if(file.isDirectory()){
				ret = recursiveListFiles( file, ret );
			} else {
				ret.add( file );
			}
		}
		
		return ret;
	}

	private static enum InputFileType {
		GTFS, OSM, CONFIG, OTHER;
		public static InputFileType forFile(File file) {
			String name = file.getName();
			if (name.endsWith(".zip")) {
				try {
					ZipFile zip = new ZipFile(file);
					ZipEntry stopTimesEntry = zip.getEntry("stop_times.txt");
					zip.close();
					if (stopTimesEntry != null)
						return GTFS;
				} catch (Exception e) { /* fall through */
				}
			}
			if (name.endsWith(".pbf"))
				return OSM;
			if (name.endsWith(".osm"))
				return OSM;
			if (name.endsWith(".osm.xml"))
				return OSM;
			if (name.equals("Embed.properties"))
				return CONFIG;
			return OTHER;
		}
	}
}
