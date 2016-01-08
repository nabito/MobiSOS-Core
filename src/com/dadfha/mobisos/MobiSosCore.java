package com.dadfha.mobisos;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiCmd;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Bag;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.util.QueryExecUtils;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import com.lambdaworks.crypto.SCryptUtil;

import org.apache.jena.query.spatial.EntityDefinition;
import org.apache.jena.query.spatial.SpatialDatasetFactory;
import org.apache.jena.query.spatial.SpatialQuery;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MobiSosCore {
	
	// Testing Params
	public static final int TESTCASE_N = 5;
	public static final int TEST_N = 1;
	public static final int[] USER_N_TESTCASE = { 100, 100, 100, 100, 100 };
	public static final int[] CHECKIN_N_TESTCASE = { 30, 30, 30, 30, 30 };
	//final int CHECKIN_WIFI_N = 10;
	//final int CHECKIN_TAG_N = 10;
	public static final int[] WIFI_ROUTER_N_TESTCASE = { 10, 100, 1000, 10000, 100000 };
	public static final int[] TAG_N_TESTCASE = { 10, 100, 1000, 10000, 100000 };
	public static final int[] CAMERA_N_TESTCASE = { 10, 100, 1000, 10000, 100000 };
	
	public static int USER_N = 1;
	public static int CHECKIN_N = 30;
	public static int WIFI_ROUTER_N = 10;
	public static int TAG_N = 10;
	public static int CAMERA_N = 10;
	
	public static int CURRENT_TESTCASE;
	
	
	final double DISTANCE_FROM_CENTER = 10.0; // km unit
	final double WALK_DISTANCE = 1000.0; // m unit before next check-in
	final double AVG_WALK_SPEED = (25.0/18.0); // m/s unit
	final GeoLocation center = GeoLocation.fromDegrees(35.689506, 139.6917);	
	
	
	// TODO refactor to use Checkin class internally
	// Add the timing measure to result too!!! or declare global to takecare of debug and testing
	class NearByNodeResult {
		public String checkin;
		public double lat, lon;
		public long time;
		public double q1Time;
		public double q2Time;
		public double totalQueryTime;
		public List<String> nodeList;
		NearByNodeResult(String checkin, double lat, double lon, long time, List<String> nodeList, double q1Time, double q2Time, double totalQueryTime) {
			this.lat = lat; this.lon = lon; this.time = time; this.checkin = checkin; this.nodeList = nodeList; this.q1Time = q1Time; this.q2Time = q2Time; this.totalQueryTime = totalQueryTime;
		}
	}
	
	class Checkin {
		public String checkinUri;
		public GeoLocation loc;
		public long timestamp;
		Checkin(String checkinUri, GeoLocation loc, long timestamp) {
			this.checkinUri = checkinUri; this.loc = loc; this.timestamp = timestamp;
		}
	}
	
	static {
		Log.setLog4j();
	}
	static Logger log = LoggerFactory.getLogger("MobiSosCoreLogger");	
	
	private static final String FUSEKI_HOME_PATH = "/Users/Wirawit/dev/jena-fuseki-1.0.0";
	// TODO Upgrade to Fuseki 2.x for Security support, Admin UI, and etc. 
	//private static final String FUSEKI_HOME_PATH = "/Users/Wirawit/dev/apache-jena-fuseki-2.3.1";
	private static final String TDB_PATH = FUSEKI_HOME_PATH + "/MobiSosTDB";
	private static final String TDB_INDEX_PATH = FUSEKI_HOME_PATH + "/MobiSosTdbIndex";
	private static final File TDB_DIR = new File(TDB_PATH);
	private static final File TDB_INDEX_DIR = new File(TDB_INDEX_PATH);
	
	// IMP Put URIs and Prefixes in a map collection for auto population  
	protected static final String URI_BASE = "http://www.dadfha.com/mobisos-rdf/1.0#";
	private static final String URI_W3C_GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final String URI_JENA_SPATIAL = "http://jena.apache.org/spatial#";
	
	// Prefix must be a valid NCName - http://en.wikipedia.org/wiki/QName
	private static final String URI_PREFIX_BASE = "mbs";
	private static final String URI_PREFIX_RDF = "rdf";
	private static final String URI_PREFIX_RDFS = "rdfs";
	private static final String URI_PREFIX_XSD = "xsd";
	private static final String URI_PREFIX_W3C_GEO = "geo";
	private static final String URI_PREFIX_JENA_SPATIAL = "spatial";
	
	// SPARQL query prefix
	private static final String QUERY_PREFIX_MAIN = StrUtils.strjoinNL("PREFIX " + URI_PREFIX_BASE + ": <" + URI_BASE + ">",
			//"PREFIX " + URI_PREFIX_JENA_SPATIAL + ": <" + URI_JENA_SPATIAL + ">",
			"PREFIX " + URI_PREFIX_RDF + ": <"+ RDF.getURI() +">",
			"PREFIX " + URI_PREFIX_RDFS + ": <"+ RDFS.getURI() +">",
			"PREFIX " + URI_PREFIX_XSD + ": <" + XSD.getURI() + ">"
			);
	
	/*
	 * IMP make use of this native function to construct long string is more readable
	String str = String.format("Action %s occured on object %s.",
   		objectA.getAction(), objectB);
	*/
	
	private Dataset dataset;
	private Model model;
	
	public static final Resource RES_NODE  = resource("node");
	public static final Resource RES_USER  = resource("user");
	public static final Resource RES_CHK_REC  = resource("chk-record");
	public static final Resource RES_DEV_BAG  = resource("dev-bag");
	public static final Resource RES_DEV  = resource("dev");
	public static final Resource RES_CHK  = resource("chk");
	public static final Resource RES_WIFI  = resource("wifi");
	public static final Resource RES_CAMERA  = resource("cam");
	public static final Resource RES_UC_TAG  = resource("uctag");
	public static final Resource RES_UBIX  = resource("ubix"); // The ubiquitous explorer
	
	public static final Resource RES_TRACK_SERV  = resource("TrackService");
	public static final Resource RES_VDO_SERV  = resource("VideoService");
	public static final Resource RES_SERV_LIST = resource("serv-list");
	
	public static final Property PROP_UUID = property("uuid");
	public static final Property PROP_UDID = property("udid");
	public static final Property PROP_AUTH_KEY = property("authKey");
	public static final Property PROP_HAS_CHECKIN = property("hasCheckin");
	public static final Property PROP_HAS_DEVICE = property("hasDevice");
	public static final Property PROP_VIA = property("via");
	public static final Property PROP_TIMESTAMP = property("timestamp");
	public static final Property PROP_GEO_LOC = property("geolocation");
	public static final Property PROP_UCODE = property("ucode");

	public static final Property PROP_MAC_ADDR = property("macAddress");
	public static final Property PROP_SSID = property("ssid");
	public static final Property PROP_LAST_UPDATE = property("lastUpdate");
	public static final Property PROP_LAST_SEEN = property("lastSeen");
	public static final Property PROP_L10N_METHOD = property("l10nMethod");
	
	public static final Property PROP_HAS_SERV = property("hasService");
	
	
	// GeoLocation related properties
	public static final Property PROP_ACCU = property("accuracy");
	public static final Property PROP_ALTI = property("altitude");
	public static final Property PROP_ALTI_ACCU = property("altitudeAccuracy");
	public static final Property PROP_HEADING = property("heading");
	public static final Property PROP_LAT = property("lat"); // radian unit
	public static final Property PROP_LON = property("lon");
	public static final Property PROP_SPEED = property("speed");
	public static final Property PROP_LOC_NAME = property("locationName");
	
	public static final Property PROP_W3C_LAT = property(URI_W3C_GEO, "lat"); // degree unit (so not used in our system now)
	public static final Property PROP_W3C_LON = property(URI_W3C_GEO, "long");
	
	/**
	 * Radius range to lookup for nodes in KiloMeter unit
	 */
	private static final double NODE_LOOKUP_RANGE = 1.0;
	
	/**
	 * Limit number of returned node queried result for each check-in
	 */
	private static final int NODE_RESULT_N = 10;
	
	private static final int SCRYPT_PARAM_N = 16384;
	private static final int SCRYPT_PARAM_R = 8;
	private static final int SCRYPT_PARAM_P = 1;
	
	/**
	 * milliseconds in on day
	 */
	private static final long ONEDAY_MS = 86400000L;
	
	private static final Gson gson = new Gson();
	
    protected static final Resource resource( String local )
    { return ResourceFactory.createResource( URI_BASE + local ); }
    
    protected static final Resource resource( String uri, String local )
    { return ResourceFactory.createResource( uri + local ); }	    
	
    protected static final Property property( String local )
    { return ResourceFactory.createProperty( URI_BASE, local ); }
    
    protected static final Property property( String uri, String local )
    { return ResourceFactory.createProperty( uri, local ); }	    
	
    public MobiSosCore() throws IOException {
    	this(true, false);
    }
    
	public MobiSosCore(boolean enableFuseki, boolean preserveOldData) throws IOException {
		
		if(enableFuseki == true) {
			// Fuseki start-up
			new Thread(new Runnable(){
				@Override
				public void run() {
					// Init the Fuseki
			    	String[] args = new String[4];
			    	args[0] = "--loc=" + TDB_PATH;
			    	args[1] = "--home=" + FUSEKI_HOME_PATH;	
			    	args[2] = "--update";	
			    	args[3] = "/mobisos";		
			    	
			        // Just to make sure ...
			        ARQ.init() ;
			        TDB.init() ;
			        Fuseki.init() ;
			        new FusekiCmd(args).mainRun();				
				}
				
			}).start();			
		}
		
		dataset = initDbDataset(TDB_DIR, preserveOldData);
		//dataset = initTDBDatasetWithLuceneSpatitalIndex(TDB_INDEX_DIR, TDB_DIR, false);

		//loadData(dataset, "/Users/Wirawit/dev/jena-fuseki-1.0.0/geoarq-data-1.ttl");		
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		// IMP consider adding rdfs:label and rdfs:comment for each of property also rdf:type rdf:Property is desirable 
		// so it's become sentences and can be saved to TDB (also do the same with our defined resource)
		
		// IMP consider naming graph for better management/performance? at least sensor DB should get separated from app db
		// dataset.addNamedModel(NS_MOBISOS + NS_PREFIX_MOBISOS, model);
		model.setNsPrefix(URI_PREFIX_BASE, URI_BASE);
		model.setNsPrefix(URI_PREFIX_RDF, RDF.getURI());
		//model.setNsPrefix(URI_PREFIX_W3C_GEO, URI_W3C_GEO);
		model.setNsPrefix(URI_PREFIX_XSD, XSD.getURI());
		model.setNsPrefix(URI_PREFIX_RDFS, RDFS.getURI());

		model.close();		
		dataset.commit();
		dataset.end();		

	}
	
	public String[] generateTestData(double distance, double walkDistance, double avgWalkSpeed, int userN, int checkinN, int wifiRouterN, int tagN, int cameraN) {
		
		String[] users = new String[userN];
		final Random randy = new Random();
		GeoLocation randomLoc = null;
		
		String macAddr;
		ArrayList<String> macAddrList = new ArrayList<String>();
		
		//String uuid = createUser("tester@dadfha.com", "passwd", generateUUID());
		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();

		for(int i=0; i < wifiRouterN; i++) {
			randomLoc = GeoLocation.randomCoordInDistance(distance, center);
			macAddr = generateMacAddress(false);
			macAddrList.add(macAddr);
			createWifiResource(model, macAddr, randomLoc, true);	
		}		
		
		for(int i=0; i < tagN; i++) {
			randomLoc = GeoLocation.randomCoordInDistance(distance, center);			
			createTagResource(model, generateUUID(), randomLoc);
		}		
		
		for(int i=0; i < cameraN; i++) {
			randomLoc = GeoLocation.randomCoordInDistance(distance, center);			
			createCameraResource(model, generateUUID(), randomLoc);			
		}
		
		model.close();		
		dataset.commit();
		dataset.end();
		
		for(int i=0; i < userN; i++) {
			users[i] = generateTestUserRecord( Long.toHexString(Double.doubleToLongBits(Math.random())), GeoLocation.randomCoordInDistance(distance, center, 0L), walkDistance, avgWalkSpeed, checkinN );
		}
		
		return users;
		
	}
	
	public String generateTestUserRecord(String username, GeoLocation startLocation, double walkDistance, double avgWalkSpeed, int checkinN) {
		String uuid = createUser(username, "passwd", generateUUID());
		
		long startTime = System.currentTimeMillis();
		startLocation.setTimestamp(startTime);
		GeoLocation newLoc = startLocation;
		// t = s/v = 100m / (5km/hr) = 100 / (5000m/3600s) = 100 / (25/18) = 1800/25 = 72,000ms
		int deltaT = (int) ( ( walkDistance / avgWalkSpeed ) * 1000 ); // in ms
		
		for(int i=0; i < checkinN; i++) {
			try {		
				checkin(uuid, newLoc);				
			} catch (Exception e) {				
				e.printStackTrace();
			}
			// IMP create mobile simulation with configurable ave speed, distance before changing direction and plot on google earth (or map street?)
			startTime += deltaT;
			newLoc = GeoLocation.randomCoordInDistance(walkDistance, newLoc, startTime);	
		}
		
		/*
		for(int i=0; i < CHECKIN_WIFI_N; i++) {			
			try {
				// randomly choose from known Wifi in the environment
				checkinWifi(uuid, newLoc, macAddrList.get(randy.nextInt(macAddrList.size() + 1)));				
			} catch (Exception e) {				
				e.printStackTrace();
			}
			// IMP create mobile simulation with configurable ave speed, distance before changing direction and plot on google earth (or map street?)
			startTime += deltaT;
			newLoc = GeoLocation.randomCoordInDistance(WALK_DISTANCE, newLoc, startTime);
		}	
		*/			
		
		return uuid;
	}
	
	public void beginTest() throws IOException {
				
		Log.disable("MobiSosCoreLogger");
		
		double avgQ1Time = 0.0, nowQ1Time = 0.0;
		double avgQ2Time = 0.0, nowQ2Time = 0.0;
		double avgTotalQueryTime = 0.0, nowTotalQueryTime = 0.0;
		
		String[] uuid = generateTestData(DISTANCE_FROM_CENTER, WALK_DISTANCE, AVG_WALK_SPEED, USER_N, CHECKIN_N, WIFI_ROUTER_N, TAG_N, CAMERA_N);

		for(int i=0; i < TEST_N; i++) {
		
			for(int j=0; j < USER_N; j++) {
				
				ArrayList<NearByNodeResult> res = (ArrayList<NearByNodeResult>) findNearbyNodes(uuid[j], NODE_LOOKUP_RANGE, (System.currentTimeMillis() - ONEDAY_MS), System.currentTimeMillis());
				
				int checkinCount = 0;
				nowQ1Time = 0.0;
				nowQ2Time = 0.0;
				nowTotalQueryTime = 0.0;
				
				for(NearByNodeResult nbn: res) {
					checkinCount++;
					String s = String.format("%n%d. Checkin: %s%n Lat: %s%n Lon: %s%n Time: %s%n", checkinCount, nbn.checkin, nbn.lat, nbn.lon, nbn.time);
					log.info(s);
					log.info(" Near by node(s):");
					for(String node: nbn.nodeList) {
						log.info("  " + node);
					}
					nowQ1Time = nbn.q1Time;
					nowQ2Time += nbn.q2Time;
					nowTotalQueryTime += nbn.totalQueryTime;
				}
				
				avgQ1Time += nowQ1Time;
				avgQ2Time += (nowQ2Time / checkinCount);
				avgTotalQueryTime += (nowTotalQueryTime / checkinCount);				
				
			}
			
		}
		
		avgQ1Time /= (TEST_N * USER_N);
		avgQ2Time /= (TEST_N * USER_N);
		avgTotalQueryTime /= (TEST_N * USER_N);
		
		String summary = String.format("%n Average Query1 Time: %f%n Average Query2 Time: %f%n Average Total Query Time: %f%n", avgQ1Time, avgQ2Time, avgTotalQueryTime);
		System.out.println(summary);
		
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy h:mm:ss a");
		log.info(sdf.format(date));
		String filePrefix = sdf.format(date) + " Test" + CURRENT_TESTCASE;
		//exportModelToFile(filePrefix + ".ttl", "TTL");
		
		CSVWriter writer = new CSVWriter(new FileWriter(filePrefix + ".csv"), ',');
	    // feed in your array (or convert your data to an array)
	    String[] entries = { Long.toString(model.size()), Double.toString(avgQ1Time), Double.toString(avgQ2Time), Double.toString(avgTotalQueryTime) };
	    writer.writeNext(entries);
		writer.close();

	}
	
	public void sosCall(String uuid, String udid, String location) throws IOException {
		
		log.info("SOS called on mobiSOS-Core");
		
		boolean success = true;
		
		// update latest checkin to DB
		if(location != null) {
			try {
				checkin(uuid, location);
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		// IMP turn user's SOS flag on
		
		// query semantic DB for nearby nodes from 24-hours before until now
		ArrayList<NearByNodeResult> res = (ArrayList<NearByNodeResult>) findNearbyNodes(uuid, NODE_LOOKUP_RANGE, (System.currentTimeMillis() - ONEDAY_MS), System.currentTimeMillis());
		
		int counter = 0;
		for(NearByNodeResult nbn: res) {
			counter++;
			String s = String.format("%n%d. Checkin: %s%n Lat: %s%n Lon: %s%n Time: %s%n", counter, nbn.checkin, nbn.lat, nbn.lon, nbn.time);
			System.out.println(s);
			System.out.println(" Near by node(s):");
			for(String node: nbn.nodeList) {
				System.out.println("  " + node);
			}
		}
		
		if(!success) throw new RuntimeException("Error! Can't complete sos call operation.");
		
		
		// then ask for tracking record "Do you see Bob?"		
	}
	
	
	private List<NearByNodeResult> findNearbyNodes(String uuid, double distance) {
		return findNearbyNodes(uuid, distance, 0L, 0L);
	}
	
	/**
	 * findNearbyNodes using haversine formula + bounding coordinates technique
	 * @param uuid
	 * @param distance the radius length in KM to look for near by nodes WRT each check-in location
	 * @param since record lookup start time (time unit in milliseconds since midnight, January 1, 1970 UTC.) Value 0L will cause the function to ignore start time filter.
	 * @param until record lookup end time (time unit in milliseconds since midnight, January 1, 1970 UTC.) Value 0L will cause the function to ignore stop time filter.
	 * @return List<NearByNodeResult> list of near-by-node finding result
	 */
	private List<NearByNodeResult> findNearbyNodes(String uuid, double distance, long since, long until) {

		ArrayList<NearByNodeResult> nbnList = new ArrayList<NearByNodeResult>();
		
		String sinceCond = "";
		String untilCond = "";
		String andCond = "";
		String filter = "";
		if(since != 0L) sinceCond = "?time >= " + since;
		if(since != 0L) untilCond = "?time <= " + until;
		if(since != 0L && until != 0L) andCond = " && ";
		if(since != 0L || until != 0L) filter = "FILTER (" + sinceCond + andCond + untilCond + ")";
		
		String qs1 = StrUtils.strjoinNL("SELECT ?lat ?lon ?checkin ?time", "WHERE",			
				"{",
				"<%s.%s> <%s> ?seq .",
				"?seq ?ord ?checkin .",
				"?checkin <%s> ?bn .",
				"?checkin <%s> ?time .",
				"?bn <%s> ?lat .",
				"?bn <%s> ?lon .",
				filter,
				"}", 
				"ORDER BY DESC(?time)"
				);		
		
		qs1 = String.format(qs1,
				RES_USER.getURI(), uuid, PROP_HAS_CHECKIN.getURI(),
				PROP_GEO_LOC.getURI(),
				PROP_TIMESTAMP.getURI(),
				PROP_LAT.getURI(),
				PROP_LON.getURI()
		   		);		
		
		// First, query for check-ins	
		dataset.begin(ReadWrite.READ);
		long startTime = 0L;
		long finishTime = 0L;
		double q1Time = 0.0;
		double q2Time = 0.0;
		double totalQueryTime = 0.0;
		int checkinCount = 0;
		int nearbyNodeCount = 0;
		Query q1 = null;
		QueryExecution qexec1 = null;
		Query q2 = null;
		QueryExecution qexec2 = null;
	
		try {
			
			qs1 = QUERY_PREFIX_MAIN + System.lineSeparator() + qs1;			
			//System.out.println(System.lineSeparator() + qs1);
			
			log.info(String.format("START Check-in Query"));
			startTime = System.nanoTime();
			
			q1 = QueryFactory.create(qs1);
			qexec1 = QueryExecutionFactory.create(q1, dataset);
		    ResultSet chkResults = qexec1.execSelect();
		    
			finishTime = System.nanoTime();
			q1Time = (finishTime - startTime) / 1.0e6;
			log.info(String.format("FINISH Check-in Query - %.2fms", q1Time));

			for (; chkResults.hasNext();) {
				
				checkinCount++;
				
				QuerySolution chkSoln = chkResults.nextSolution();
				Literal lati = chkSoln.getLiteral("lat"); // Get a result variable by name.
				Literal longi = chkSoln.getLiteral("lon");
				Literal time = chkSoln.getLiteral("time");
				Resource checkin = chkSoln.getResource("checkin");
				
				GeoLocation loc = GeoLocation.fromRadians(lati.getDouble(), longi.getDouble());
				GeoLocation[] boundingCoordinates = loc.boundingCoordinates(distance, GeoLocation.R);
				
				boolean meridian180WithinDistance =
						boundingCoordinates[0].getLongitudeInRadians() >
						boundingCoordinates[1].getLongitudeInRadians();
				
				String boolConj = "&&";
				if(meridian180WithinDistance) boolConj = "||"; 

				// Second, for each checkin-lat-long, query for nodes within circle!
				
				String qs2 = StrUtils.strjoinNL("SELECT DISTINCT ?node ?lat ?lon", "WHERE", "{", 
							"?node <%s> ?bn ;",
							"a <%s> .",
							"?bn <%s> ?lat .",
							"?bn <%s> ?lon .",
							"FILTER ( (?lat >= %s && ?lat <= %s ) && (?lon >= %s %s ?lon <= %s) )",
							"}",
							"LIMIT %s");
							
				qs2 = String.format(qs2,
						PROP_GEO_LOC.getURI(),
						RES_NODE.getURI(),
						PROP_LAT.getURI(),
						PROP_LON.getURI(),
						boundingCoordinates[0].getLatitudeInRadians(), boundingCoordinates[1].getLatitudeInRadians(), 
						boundingCoordinates[0].getLongitudeInRadians(), boolConj, boundingCoordinates[1].getLongitudeInRadians(),
						NODE_RESULT_N
						);
				
				/*
				String qs2 = StrUtils.strjoinNL("SELECT DISTINCT ?node ?lat ?lon IF(COUNT(?servIdx) > 0) AS ?hasTrackServ", "WHERE", "{", 
						"?node <%s> ?bn ;",
						"a <%s> .",
						"?bn <%s> ?lat .",
						"?bn <%s> ?lon .",
						"OPTIONAL { ?node <%s> ?serv . }", // a node may/may not have a service
						"OPTIONAL { ?serv ?servIdx <%s> . }", // if there is a service, it must be of a specific type
						"FILTER ( (?lat >= %s && ?lat <= %s ) && (?lon >= %s %s ?lon <= %s) )",
						"}");				
				// if a node has more than one tracking service, only one will be selected here
				// does distinct also apply to ?lat ?lon ?
				
				qs2 = String.format(qs2,
						PROP_GEO_LOC.getURI(),
						RES_NODE.getURI(),
						PROP_LAT.getURI(),
						PROP_LON.getURI(),
						PROP_HAS_SERV.getURI(),
						//RES_TRACK_SERV.getURI(),
						RES_VDO_SERV.getURI(),
						boundingCoordinates[0].getLatitudeInRadians(), boundingCoordinates[1].getLatitudeInRadians(), 
						boundingCoordinates[0].getLongitudeInRadians(), boolConj, boundingCoordinates[1].getLongitudeInRadians()
						);
						*/

				qs2 = QUERY_PREFIX_MAIN + System.lineSeparator() + qs2;
				//System.out.println(System.lineSeparator() + qs2);

				log.info(String.format("START Node Query for: %s", checkin.getURI()));
				startTime = System.nanoTime();
				
				q2 = QueryFactory.create(qs2);
				qexec2 = QueryExecutionFactory.create(q2, dataset);
				ResultSet nodeResults = qexec2.execSelect();
				
				finishTime = System.nanoTime();
				q2Time = (finishTime - startTime) / 1.0e6;
				totalQueryTime = q1Time + q2Time;
				log.info(String.format("FINISH Node Query - %.2fms", q2Time));
				log.info(String.format("Total Query Time - %.2fms", totalQueryTime));
				
				//QueryExecUtils.executeQuery(q2, qexec2);
				List<String> nodeList = new ArrayList<String>();

				for (; nodeResults.hasNext();) {
					
					QuerySolution nodeSoln = nodeResults.nextSolution();
					Resource node = nodeSoln.getResource("node");
					Literal nodeLat = nodeSoln.getLiteral("lat"); // Get a result variable by name.
					Literal nodeLon = nodeSoln.getLiteral("lon");
					
					GeoLocation nodeLoc = GeoLocation.fromRadians(nodeLat.getDouble(), nodeLon.getDouble());
					
					// filter out the out of ring node					
					if(GeoLocation.haversine(loc, nodeLoc) <= distance) {
						
						nearbyNodeCount++;
						
						//System.out.println(node.toString());
						// add each node to array list
						nodeList.add(node.toString());
					}

				} // end each node withinCircle for
				
				// add all nodes within circle to the result
				//if(nodeList.size() > 0)
					nbnList.add(new NearByNodeResult(checkin.getURI(), lati.getDouble(), longi.getDouble(), time.getLong(), nodeList, q1Time, q2Time, totalQueryTime));

			} // end each check-in for
			
			log.info("Summary:");
			log.info("Check-in result# " + checkinCount);
			log.info("Nearby Node result# " + nearbyNodeCount);
			
		    
		} finally {
			if(qexec1 != null) qexec1.close();
			if(qexec2 != null) qexec2.close();
			dataset.end();
		}
		
		return nbnList;
	}
	
	public ArrayList<Checkin> getCheckin(String uuid, long since, long until) {
		
		ArrayList<Checkin> checkins = new ArrayList<Checkin>();
		
		String sinceCond = "";
		String untilCond = "";
		String andCond = "";
		String filter = "";
		if(since != 0L) sinceCond = "?time >= " + since;
		if(since != 0L) untilCond = "?time <= " + until;
		if(since != 0L && until != 0L) andCond = " && ";
		if(since != 0L || until != 0L) filter = "FILTER (" + sinceCond + andCond + untilCond + ")";
		
		String qs1 = StrUtils.strjoinNL("SELECT ?checkin ?lat ?lon ?time", "WHERE",			
				"{",
				"<%s.%s> <%s> ?seq .",
				"?seq ?ord ?checkin .",
				"?checkin <%s> ?bn .",
				"?checkin <%s> ?time .",
				"?bn <%s> ?lat .",
				"?bn <%s> ?lon .",
				filter,
				"}", 
				"ORDER BY DESC(?time)"
				);					
		
		qs1 = String.format(qs1,
				RES_USER.getURI(), uuid, PROP_HAS_CHECKIN.getURI(),
				PROP_GEO_LOC.getURI(),
				PROP_TIMESTAMP.getURI(),
				PROP_LAT.getURI(),
				PROP_LON.getURI()
		   		);
		
		// First, query for check-ins	
		dataset.begin(ReadWrite.READ);
		long startTime = System.nanoTime();
		Query q = null;
		QueryExecution qexec = null;		
		
		try {
			
			qs1 = QUERY_PREFIX_MAIN + System.lineSeparator() + qs1;
			
			log.info(System.lineSeparator() + qs1);
			
			q = QueryFactory.create(qs1);
			qexec = QueryExecutionFactory.create(q, dataset);
		    ResultSet chkResults = qexec.execSelect();
		    
		    //log.info(ResultSetFormatter.asText(chkResults));

			for (; chkResults.hasNext();) {
				QuerySolution chkSoln = chkResults.nextSolution();				
				Resource checkin = chkSoln.getResource("checkin");
				Literal lati = chkSoln.getLiteral("lat");
				Literal longi = chkSoln.getLiteral("lon");
				Literal time = chkSoln.getLiteral("time");

				checkins.add(new Checkin(checkin.getURI(), GeoLocation.fromRadians(lati.getDouble(), longi.getDouble()), time.getLong()));
	
			}
		} finally {
			if(qexec != null) qexec.close();
			dataset.end();
		}
		
		long finishTime = System.nanoTime();
		double time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));
		
		return checkins;

	}
	
	public String getCheckinJson(String uuid, long since, long until) {
		Collection<Checkin> checkins = getCheckin(uuid, since, until);
		String json = gson.toJson(checkins);
		log.info(json);
		return json;
	}
	
	
	public void generateMashup() {
		// TODO map with tracking update in real-time, video always connect, latest status/info summary, archive log, near by user 
	}
	
	// IMP look more into genUUID() mechanism
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}
	
	/**
	 * generate 48-bit MAC address
	 * @param semicolon whether or not output MAC address is separated by ':' for each byte
	 * @return
	 */
	private static String generateMacAddress(boolean semicolon) {		
		int[] i = { (int)(Math.random() * 256), (int)(Math.random() * 256), (int)(Math.random() * 256), (int)(Math.random() * 256), (int)(Math.random() * 256), (int)(Math.random() * 256) };
		String macAddr;
		if(semicolon == true) macAddr = String.format("%02X:%02X:%02X:%02X:%02X:%02X", i[0], i[1], i[2], i[3], i[4], i[5]);
		else macAddr = String.format("%02X%02X%02X%02X%02X%02X", i[0], i[1], i[2], i[3], i[4], i[5]);
		return macAddr;
	}
	
	/**
	 * check-in
	 * @param uuid
	 * @param location
	 * @throws Exception
	 */
	private void checkin(String uuid, String location) throws Exception {
		GeoLocation geoLoc = GeoLocation.fromTitaLoc(gson.fromJson(location, TitaniumLocation.class));		
		checkin(uuid, geoLoc);
	}
	
	/**
	 * check-in
	 * @param uuid
	 * @param location
	 * @throws Exception
	 */
	private void checkin(String uuid, GeoLocation location) throws Exception {
		checkinWifi(uuid, location, null);
	}
	
	/**
	 * This method is expected to be called from a mobile client to do self check-in
	 * since Wifi router doesn't know about uid 
	 * 
	 * IMP password/API token must be accompany so any person has uuid won't be able to disguise check-in
	 * 
	 * @param uuid user ID
	 * @param location the check-in location in JSON. Structure follow that of Titanium3.x 
	 * http://docs.appcelerator.com/titanium/latest/#!/api/LocationCoordinates
	 * @param macAddr router's MAC address (BSSID) in Hexadecimal without ':'
	 * @throws Exception 
	 */
	public void checkinWifi(String uuid, String location, String macAddr) throws Exception {
		// Extract location's parameters from JSON
		GeoLocation geoLoc = GeoLocation.fromTitaLoc(gson.fromJson(location, TitaniumLocation.class));
		// IMP should we get location name from wifi check-in in the future? -- see reverse geocoding in Titanium API
		checkin(uuid, geoLoc);
		checkinWifi(uuid, geoLoc, macAddr);
	}

	public void checkinWifi(String uuid, GeoLocation location, String macAddr) throws Exception {	
		
		// TODO if no location provided, infer location from Wifi!

		// This either create or connect to existing dataset		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		// Jena createResource() will also get existing resource if already available
		// Note: ordered index is not used/generated here due to possible racing condition
		// which could break the true order of event, rather we rely on SEQ collection order
		// meaning that Seq.add() must be Synchronous operation
		
		Resource user = model.createResource(RES_USER.getURI() + "." + uuid);
		
		// Only allow authorized user to check-in
		if(!model.contains(user, PROP_UUID, uuid)) {
			log.warn("The Wifi Check-In is attemped by non-existing user!");
			System.out.println("Wifi Check-in Security Trapped!");
			model.close();
			dataset.abort();
			dataset.end();			
			throw new Exception("Non-Authorized Wifi Check-In");
		}
		
		// Get check-in records (SEQ) of the user
		Seq chkRecords = user.getProperty(PROP_HAS_CHECKIN).getSeq(); 
		if(chkRecords == null) throw new RuntimeException("Possibly Data Error! This user still has no check-in record yet!");
						
		Resource checkin = model.createResource(RES_CHK.getURI() + "." + generateUUID())
				.addLiteral(PROP_TIMESTAMP, System.currentTimeMillis())
				.addProperty(PROP_GEO_LOC, createLocBnode(model, location));
		
		if(macAddr != null) {
			// This will also create this wifi resource if not available before (Wardriving like)
			Resource wifi = createWifiResource(model, macAddr, location);			
			checkin.addProperty(PROP_VIA, wifi);
		}
		
		chkRecords.add(checkin);		
		
		model.close();
		dataset.commit();
		dataset.end();

	}
	
	/**
	 * Create a new Wifi resource or return existing one if already available in DB
	 * IMP will include ssid in the future
	 * Caution! This method must be called within a WRITE transaction. 
	 * @param model
	 * @param macAddr
	 * @param hasTrackingService
	 * @return Resource of wifi router
	 */
	private Resource createWifiResource(Model model, String macAddr, GeoLocation geoLoc, boolean hasTrackingService) {
		
		Resource wifi = model.createResource(RES_WIFI.getURI() + "." + macAddr);
		// check if this wifi router is already registered
		if(model.contains(wifi, PROP_MAC_ADDR, macAddr)) {			
			// if exists, just update wifi location as needed
			updateWifiLocation(model, wifi, macAddr, geoLoc);

		} else { // new wifi found, create triples for it
			wifi.addProperty(PROP_MAC_ADDR, macAddr)
			.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc))
			.addProperty(RDF.type, RES_NODE)
			//.addProperty(PROP_SSID, ssid)
			.addLiteral(PROP_LAST_UPDATE, System.currentTimeMillis());

			// some Wifi can provide people tracking service
			if(hasTrackingService == true) {
				Bag serviceBag = model.createBag(RES_SERV_LIST.getURI() + ".wifi." + macAddr);
				serviceBag.add(RES_TRACK_SERV);				
				wifi.addProperty(PROP_HAS_SERV, serviceBag);
			}
		}
		return wifi;
	}
	
	/**
	 * Create a new Wifi resource or return existing one if already available in DB
	 * IMP will include ssid in the future
	 * Caution! This method must be called within a WRITE transaction. 
	 * @param model
	 * @param macAddr
	 * @return Resource of wifi router
	 */	
	private Resource createWifiResource(Model model, String macAddr, GeoLocation geoLoc) {
		return createWifiResource(model, macAddr, geoLoc, false);
	}
	
	/**
	 * Create geo-location blank node 
	 * Caution! This method must be called within a WRITE transaction.
	 * @param model
	 * @return
	 */
	private Resource createLocBnode(Model model, GeoLocation geoLoc) {		
		Resource locBnode = model.createResource();
		locBnode.addLiteral(PROP_ACCU, geoLoc.getAccuracy())
				.addLiteral(PROP_ALTI, geoLoc.getAltitude())
				.addLiteral(PROP_ALTI_ACCU, geoLoc.getAltitudeAccuracy())
				.addLiteral(PROP_HEADING, geoLoc.getHeading())
				.addLiteral(PROP_LAT, geoLoc.getLatitudeInRadians())
				.addLiteral(PROP_LON, geoLoc.getLongitudeInRadians())
				.addLiteral(PROP_SPEED, geoLoc.getSpeed())
				.addLiteral(PROP_LOC_NAME, geoLoc.getLocationName()); 		
		return locBnode;
	}
	
	/**
	 * This method is expected to be called from a Wifi router to track near by/connected user
	 * by sending user's MAC address to app server for resolution
	 * @param devUUID user device's MAC address for Android and app's UUID for iOS >= 7 
	 * @param macAddr Router's MAC address (BSSID) to get mapped location in Hexadecimal without ':'
	 */
	public void trackWifiUser(String devUUID, String macAddr) {
		// TODO this is for when we have access to router firmware
		// update TDB for current user location at this wifi router
	}
	
	/**
	 * updateWifiLocation (Crowd-sourcing)
	 * This method will modify the wifi object.
	 * Caution! This method must be called within a WRITE transaction. 
	 * Also must already checked that this wifi resource is already in the model.
	 * @param model
	 * @param wifi
	 * @param macAddr
	 * @param geoLoc
	 */
	private void updateWifiLocation(Model model, Resource wifi, String macAddr, GeoLocation geoLoc) {
		
		Statement lastUpdateStm = wifi.getProperty(PROP_LAST_UPDATE);	
		
		// IMP check accuracy of the check-in, update by averaging with past data but given more weight to more accurate data
		if(lastUpdateStm.getLong() < System.currentTimeMillis()) { // this check is still need in case network condition may cause older update to come later
			lastUpdateStm.changeLiteralObject(System.currentTimeMillis());			
			// remove old statements about location and add new one	
			model.remove(model.listStatements(
					new SimpleSelector(wifi.getProperty(PROP_GEO_LOC).getResource(), null, (RDFNode) null)
			));
			model.remove(wifi.getProperty(PROP_GEO_LOC));
			wifi.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc));
			//.addProperty(PROP_SSID, ssid)
		}
		
	}
	
	public void checkinTag(String uuid, String ucode) throws Exception {		

		// This either create or connect to existing dataset		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		Resource user = model.createResource(RES_USER.getURI() + "." + uuid);
		
		// IMP Should only allow authorized user to check-in, not just existing user
		if(!model.contains(user, PROP_UUID, uuid)) {
			log.warn("The Tag Check-In is attemped by non-existing user!");
			System.out.println("Tag Check-in Security Trapped!");
			model.close();
			dataset.abort();
			dataset.end();			
			throw new Exception("Non-Authorized Tag Check-In");
		}
		
		// Get check-in records (SEQ) of the user
		Seq chkRecords = user.getProperty(PROP_HAS_CHECKIN).getSeq(); 
		if(chkRecords == null) throw new RuntimeException("Possibly Data Error! This user still has no check-in record yet!");

		// TODO Get tag's geographic coordinates from uid server
		// ...
		double uLat = 0.0;
		double uLon = 0.0;
		GeoLocation geoLoc = GeoLocation.fromDegrees(uLat, uLon);
		
		// Get the tag if it already available in the model, or create new one
		Resource tag = createTagResource(model, ucode, geoLoc);
				
		Resource checkin = model.createResource(RES_CHK.getURI() + "." + generateUUID())
				.addProperty(PROP_VIA, tag)
				.addLiteral(PROP_TIMESTAMP, System.currentTimeMillis()) 
				.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc));
		
		chkRecords.add(checkin);	
		
		model.close();
		dataset.commit();
		dataset.end();		
		
		
	}
		
	private Resource createTagResource(Model model, String ucode, GeoLocation geoLoc) {
		
		Resource tag = model.createResource(RES_UC_TAG.getURI() + "." + ucode, RES_NODE);		
		
		if(model.contains(tag, PROP_UCODE, ucode)) {			
			// if exists, compare old coordinate, update if it is changed
			Resource locBnode = tag.getProperty(PROP_GEO_LOC).getResource();
			double oldLat = locBnode.getProperty(PROP_LAT).getDouble();
			double oldLon = locBnode.getProperty(PROP_LON).getDouble();
			if( oldLat != geoLoc.getLatitudeInRadians() || oldLon != geoLoc.getLatitudeInRadians() ) {
				updateTagLocation(tag, geoLoc);
			}

		} else { // new tag found, create triples for it
			tag.addProperty(PROP_UCODE, ucode)
			.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc))
			.addLiteral(PROP_LAST_UPDATE, System.currentTimeMillis());
		}
		return tag;		
	}
	
	private void updateTagLocation(Resource tag, GeoLocation geoLoc) {
		Resource locBnode = tag.getProperty(PROP_GEO_LOC).getResource();
		locBnode.getProperty(PROP_LAT).changeLiteralObject(geoLoc.getLatitudeInRadians());
		locBnode.getProperty(PROP_LON).changeLiteralObject(geoLoc.getLongitudeInRadians());
	}
	
	private Resource createCameraResource(Model model, String udid, GeoLocation geoLoc) {
		
		Resource cam = model.createResource(RES_CAMERA.getURI() + "." + udid, RES_NODE);
		
		if(model.contains(cam, PROP_GEO_LOC)) {	
			if(geoLoc != null) {
				Resource locBnode = cam.getProperty(PROP_GEO_LOC).getResource();
				double oldLat = locBnode.getProperty(PROP_LAT).getDouble();
				double oldLon = locBnode.getProperty(PROP_LON).getDouble();		
				if( oldLat != geoLoc.getLatitudeInRadians() || oldLon != geoLoc.getLatitudeInRadians() ) {
					updateCamLocation(cam, geoLoc);
				}				
			}			
		} else {			
			Bag serviceBag = model.createBag(RES_SERV_LIST.getURI() + ".cam." + udid);
			serviceBag.add(RES_VDO_SERV);
			
			cam.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc))
			.addProperty(PROP_HAS_SERV, serviceBag)
			.addLiteral(PROP_LAST_UPDATE, System.currentTimeMillis());
		}
		
		return cam;
	}	
	
	private void updateCamLocation(Resource cam, GeoLocation geoLoc) {
		Resource locBnode = cam.getProperty(PROP_GEO_LOC).getResource();
		locBnode.getProperty(PROP_LAT).changeLiteralObject(geoLoc.getLatitudeInRadians());
		locBnode.getProperty(PROP_LON).changeLiteralObject(geoLoc.getLongitudeInRadians());		
	}
	
	
	public boolean changeMode() {
		// FIXME
		return true;
	}
	
	public User checkMacAddr(String addr) {
		User u = null;
		// FIXME
		return u;
	}
	
	/**
	 * Create a user based on email and password (the latter is not stored now)
	 * 
	 * IMP At the moment, having Class for a model is not a good idea.
	 * Because the model structure, in other word graph, could change at 
	 * any time with an introduction of new RDF node.    
	 * 
	 * @param email
	 * @param passwd
	 * @param udid Device's UDID
	 * @return String Unique User ID (UUID) for new/existing user or null if authentication failed for existing user
	 */
	public String createUser(String email, String passwd, String udid) {
		String uuid = null;
		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		// check if user e-mail is already available in database, if yes, authenticate the password and return old user
		ResIterator iter = model.listResourcesWithProperty(FOAF.mbox, email); 		
		if(iter.hasNext()) {
			
			Resource user = iter.next();
			String authKey = user.getProperty(PROP_AUTH_KEY).getString();						
		    if(SCryptUtil.check(passwd, authKey) == true) uuid = user.getProperty(PROP_UUID).getString();
		    
		} else { // The user is not already available, let's create new one

			uuid = generateUUID();
			
			Resource device = model.createResource(RES_DEV.getURI() + "." + udid, RES_NODE)
					.addProperty(RDF.type, RES_NODE);
						
			Bag udidBag = model.createBag(RES_DEV_BAG.getURI() + "." + uuid);
			udidBag.add(device);
			
			Resource user = model.createResource(RES_USER.getURI() + "." + uuid)
					.addProperty(PROP_HAS_CHECKIN, model.createSeq(RES_CHK_REC.getURI() + "." + uuid))
					.addProperty(PROP_HAS_DEVICE, udidBag)
					.addProperty(FOAF.mbox, email)
					.addProperty(PROP_UUID, uuid)
					.addProperty(PROP_AUTH_KEY, SCryptUtil.scrypt(passwd, SCRYPT_PARAM_N, SCRYPT_PARAM_R, SCRYPT_PARAM_P));
					// IMP should we add another level of salty-security over scrypt? if not, how do we separately store the salt?			
		}
		
		model.close();
		dataset.commit();
		dataset.end();
		
		return uuid;		
	}
	
	public String getUserById(String uid) {
		
		// TODO find a user by ID
		
		return gson.toJson(null);
	}
	
	/**
	 * Update a user information
	 * Remark: This method is expected to be called from Js
	 * 
	 * PROS
	 * The reason why User type is not used in method param
	 * is because it will require a call from Js side to
	 * instantiate such class just to pass a user's attribute
	 * , which could incur unnecessary processing and memory
	 * CONS
	 * not good for usage among Java programming
	 * 
	 * @param id
	 * @param user
	 * @return boolean indicating success/fail operation
	 * 
	 * Though technically possible, Exception is not useful 
	 * here due to cross-environment call
	 * 
	 */	
	public boolean updateUser(String id, String user) {
		// IMP Verify structure correctness of JSON data
		
		// Convert to Java object
		User u = gson.fromJson(user, User.class);
		
		// TODO Also, update the user in TDB		
		
		return (u.getId() != null)? true:false;
	}
	
	public boolean deleteUser(String id) {
		// TODO Delete the user from TDB
		
		
		// FIXME
		return true;
	}
	public void dumpTDB() {
		dataset.begin(ReadWrite.READ);
		model = dataset.getDefaultModel();
		
		model.write(System.out, "Turtle");
		
		model.close();
		dataset.end();
	}

	private static Dataset initDbDataset(File TDBDir, boolean preserveOldData) {
		if(!preserveOldData) {			
			deleteOldFiles(TDBDir);
			TDBDir.mkdir();		
		}
		return TDBFactory.createDataset(TDBDir.getAbsolutePath());
	}

    private static Dataset initTDBDatasetWithLuceneSpatitalIndex(File indexDir, File TDBDir, boolean preserveOldData) throws IOException{
		SpatialQuery.init();
		if(!preserveOldData) {			
			deleteOldFiles(TDBDir);
			TDBDir.mkdir();		
			deleteOldFiles(indexDir);
			indexDir.mkdirs();			
		}
		Dataset ds1 = TDBFactory.createDataset(TDBDir.getAbsolutePath());
		return joinIndexDataset(indexDir, ds1);		
    }	
    
    /**
     * deleteOldFiles
     * @param indexDir
     */
	private static void deleteOldFiles(File indexDir) {
		if (indexDir.exists())
			emptyAndDeleteDirectory(indexDir);
	}
	
	/**
	 * emptyAndDeleteDirectory
	 * @param dir
	 */
    private static void emptyAndDeleteDirectory(File dir) {
        File[] contents = dir.listFiles() ;
        if (contents != null) {
            for (File content : contents) {
                if (content.isDirectory()) {
                    emptyAndDeleteDirectory(content) ;
                } else {
                    content.delete() ;
                }
            }
        }
        dir.delete() ;
    } 
	
	/**
	 * joinIndexDataset
	 * @param indexDir
	 * @param baseDataset
	 * @return
	 * @throws IOException
	 */
	private static Dataset joinIndexDataset(File indexDir, Dataset baseDataset) throws IOException{
		EntityDefinition entDef = new EntityDefinition("entityField", "geoField");
	
		// set custom geo predicates
		entDef.addSpatialPredicatePair(PROP_LAT, PROP_LON);		
		
		// Lucene, index in File system.
		Directory dir = FSDirectory.open(indexDir);

		// Join together into a dataset (this does not re-index the base dataset)
		Dataset ds = SpatialDatasetFactory.createLucene(baseDataset, dir, entDef);

		return ds;
	}
	
	/**
	 * Load RDF data from file to TDB 
	 * @param ds
	 * @param file
	 */
	public static void loadData(Dataset ds, String file) {
		log.info("Start loading");
		long startTime = System.nanoTime();
		ds.begin(ReadWrite.WRITE);
		try {
			Model m = ds.getDefaultModel();
			RDFDataMgr.read(m, file);
			ds.commit();
		} finally {
			ds.end();
		}

		long finishTime = System.nanoTime();
		double time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("Finish loading - %.2fms", time));
	}
	
	public void exportModelToFile(String filename, String format) {
		dataset.begin(ReadWrite.READ);
		model = dataset.getDefaultModel();

		String fileName = filename;
		FileWriter out = null;
		try {
			out = new FileWriter(fileName);
			model.write(out, format);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			model.close();
			dataset.commit();
			dataset.end();
			try {
				out.close();
			} catch (IOException closeException) {
				closeException.printStackTrace();
			}
		}
	}

	public void addSamples() {
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
				
		//model = ModelFactory.createDefaultModel();
		Resource r = model.createResource(URI_BASE + "tokyo");

		model.addLiteral(r, PROP_LAT, 37.78583f);
		model.addLiteral(r, PROP_LON, -122.40641f);
		
		Resource r2 = model.createResource(URI_BASE + "near.tokyo");

		model.addLiteral(r2, PROP_LAT, 37.000);
		model.addLiteral(r2, PROP_LON, -122.000);			
				
		model.close();
		dataset.commit();
		dataset.end();
	
	}


}
