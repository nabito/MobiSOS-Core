package com.dadfha.mobisos;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiCmd;

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
import com.hp.hpl.jena.rdf.model.Bag;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
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
	
	static {
		Log.setLog4j();
	}
	static Logger log = LoggerFactory.getLogger("MobiSosCoreLogger");	
	
	private static final String TDB_PATH = "/Users/Wirawit/dev/jena-fuseki-1.0.0/MobiSosTDB";
	private static final String TDB_INDEX_PATH = "/Users/Wirawit/dev/jena-fuseki-1.0.0/MobiSosTdbIndex";
	private static final File TDB_DIR = new File(TDB_PATH);
	private static final File TDB_INDEX_DIR = new File(TDB_INDEX_PATH);
	
	// IMP Put URIs and Prefixes in a map collection for auto population  
	protected static final String URI_BASE = "http://www.dadfha.com/mobisos-rdf/1.0#";
	private static final String URI_W3C_GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final String URI_JENA_SPATIAL = "http://jena.apache.org/spatial#";
	
	// Prefix must be a valid NCName - http://en.wikipedia.org/wiki/QName
	private static final String URI_PREFIX_BASE = "mbs";
	private static final String URI_PREFIX_RDFS = "rdfs";
	private static final String URI_PREFIX_W3C_GEO = "geo";
	private static final String URI_PREFIX_JENA_SPATIAL = "spatial";
	
	// SPARQL query prefix
	private static final String QUERY_PREFIX_MAIN = StrUtils.strjoinNL("PREFIX " + URI_PREFIX_BASE + ": <" + URI_BASE + ">",
			"PREFIX " + URI_PREFIX_JENA_SPATIAL + ": <" + URI_JENA_SPATIAL + ">",
			"PREFIX " + URI_PREFIX_RDFS + ": <"+ RDFS.getURI() +">");
	
	/*
	String precheck = StrUtils.strjoinNL("PREFIX mbs: <http://www.dadfha.com/mobisos-rdf/1.0#>",
			"PREFIX spatial: <http://jena.apache.org/spatial#>",
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");		
	
	if(pre.equals(precheck)) System.out.println("Yay"); else System.out.println("Boo"); 
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
	
	// IMP consider adding rdfs:label and rdfs:comment for each of property
	// so it's become sentences and can be saved to TDB
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
	
	// GeoLocation related properties
	public static final Property PROP_ACCU = property("accuracy");
	public static final Property PROP_ALTI = property("altitude");
	public static final Property PROP_ALTI_ACCU = property("altitudeAccuracy");
	public static final Property PROP_HEADING = property("heading");
	public static final Property PROP_LAT = property("latitude");
	public static final Property PROP_LONG = property("longitude");	
	public static final Property PROP_SPEED = property("speed");
	public static final Property PROP_LOC_NAME = property("locationName");
	
	public static final Property PROP_W3C_LAT = property(URI_W3C_GEO, "lat");
	public static final Property PROP_W3C_LONG = property(URI_W3C_GEO, "long");
	
	/**
	 * Radius range to lookup for nodes in KiloMeter unit
	 */
	private static final double NODE_LOOKUP_RANGE = 100.0;
	
	private static final int SCRYPT_PARAM_N = 16384;
	private static final int SCRYPT_PARAM_R = 8;
	private static final int SCRYPT_PARAM_P = 1;
	
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
		
		
		// Fuseki start-up
		new Thread(new Runnable(){
			@Override
			public void run() {
				// Init the Fuseki
		    	String[] args = new String[4];
		    	args[0] = "--loc=/Users/Wirawit/dev/jena-fuseki-1.0.0/MobiSosTDB/";
		    	args[1] = "--home=/Users/Wirawit/dev/jena-fuseki-1.0.0/";	
		    	args[2] = "--update";	
		    	args[3] = "/mobisos";		
		    	
		        // Just to make sure ...
		        ARQ.init() ;
		        TDB.init() ;
		        Fuseki.init() ;
		        new FusekiCmd(args).mainRun();				
			}
			
		}).start();
		
		
		dataset = initTDBDatasetWithLuceneSpatitalIndex(TDB_INDEX_DIR, TDB_DIR, false);

		//loadData(dataset, "/Users/Wirawit/dev/jena-fuseki-1.0.0/geoarq-data-1.ttl");		
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		// IMP consider naming graph for better management/performance? at least sensor DB should get separated from app db
		// dataset.addNamedModel(NS_MOBISOS + NS_PREFIX_MOBISOS, model);
		model.setNsPrefix(URI_PREFIX_BASE, URI_BASE);
		model.setNsPrefix(URI_PREFIX_W3C_GEO, URI_W3C_GEO);				

		model.close();		
		dataset.commit();
		dataset.end();
		
		
	}
	
	public void sosCall(String uid, String udid, String location) throws IOException {
		
		System.out.println("SOS called on mobiSOS-Core");
		
		boolean success = true;
		
		// update latest checkin to DB
		checkInGps(uid, udid, location);
		
		// TODO turn user's SOS flag on
		
		// query semantic DB for nearby nodes
		//queryData(dataset);		
		//ArrayList<String> nodes = (ArrayList<String>) findNearbyNodes(uid, 1388033019876L, 1388033019876L);
		ArrayList<String> nodes = (ArrayList<String>) findNearbyNodes(uid, NODE_LOOKUP_RANGE);
		
		if(!success) throw new RuntimeException("Error! Can't complete sos call operation.");
		
		
		// then ask for tracking record "Do you see Bob?"		
	}
	
	
	private List<String> findNearbyNodes(String uid, double range) {
		return findNearbyNodes(uid, range, 0L, 0L);
	}
	
	/**
	 * findNearbyNodes
	 * @param uid user ID 
	 * @param range the radius range in KM to look for near by nodes WRT each check-in location
	 * @param since record lookup start time (time unit in milliseconds since midnight, January 1, 1970 UTC.) Value 0L will cause the function to ignore start time filter.
	 * @param until record lookup end time (time unit in milliseconds since midnight, January 1, 1970 UTC.) Value 0L will cause the function to ignore stop time filter.
	 * @return ArrayList<String> list of nearby nodes
	 */
	private List<String> findNearbyNodes(String uid, double range, long since, long until) {
		ArrayList<String> nodeList = new ArrayList<String>();
		 
		class MyQueryResult {
			RDFNode lati, longi, time, checkin;
			MyQueryResult(RDFNode lati, RDFNode longi, RDFNode time, RDFNode checkin) {
				this.lati = lati; this.longi = longi; this.time = time; this.checkin = checkin;
			}
		}
		
		ArrayList<MyQueryResult> objList = new ArrayList<MyQueryResult>();
		
		String sinceCond = "";
		String untilCond = "";
		String andCond = "";
		String filter = "";
		if(since != 0L) sinceCond = "?time >= " + since;
		if(since != 0L) untilCond = "?time <= " + until;
		if(since != 0L && until != 0L) andCond = " && ";
		if(since != 0L || until != 0L) filter = " FILTER (" + sinceCond + andCond + untilCond + ")";
		
		String qs1 = StrUtils.strjoinNL("SELECT ?lat ?long ?checkin ?time", "WHERE",			
				"{",
				"<" + RES_USER.getURI() + "." + uid + "> <" + PROP_HAS_CHECKIN.getURI() + "> ?seq .",
				"?seq ?ord ?checkin .",
				"?checkin <" + PROP_GEO_LOC.getURI() + "> ?bn .",
				"?checkin <" + PROP_TIMESTAMP.getURI() + "> ?time .",
				"?bn <" + PROP_W3C_LAT.getURI() + "> ?lat .",
				"?bn <" + PROP_W3C_LONG.getURI() + "> ?long .",
//				"?bn2 spatial:withinCircle (?lat ?long " + range + " 'km') .", // this currently is not working due to Jena spatial bug/feature?
				filter,
				"}", 
				"ORDER BY DESC(?time)"
				);
		
		// First, query for check-ins
		
		dataset.begin(ReadWrite.READ);
		long startTime = System.nanoTime();
		Query q1 = null;
		QueryExecution qexec1 = null;
	
		try {
			
			qs1 = QUERY_PREFIX_MAIN + "\n" + qs1;
			q1 = QueryFactory.create(qs1);
			
			System.out.println(q1);
			
			qexec1 = QueryExecutionFactory.create(q1, dataset);
		    ResultSet chkResults = qexec1.execSelect();

		    for ( ; chkResults.hasNext() ; )
		    {
		      QuerySolution chkSoln = chkResults.nextSolution();
		      RDFNode lati = chkSoln.get("lat");       // Get a result variable by name.
		      RDFNode longi = chkSoln.get("long");
		      RDFNode time = chkSoln.get("checkin");
		      RDFNode checkin = chkSoln.get("time");
		      //Resource r = soln.getResource("VarR") ; // Get a result variable - must be a resource
		      //Literal l = soln.getLiteral("VarL") ;   // Get a result variable - must be a literal		      
		      
		      objList.add(new MyQueryResult(lati, longi, time, checkin));
		      
		      /*
		      System.out.println(lati.toString());
		      System.out.println(longi.toString());
		      System.out.println(checkin.toString());
		      System.out.println(time.toString());
		      */
		      
		    } // end each check-in for
		    
		} finally {
			if(qexec1 != null) qexec1.close();
			dataset.end();
		}
		
		long finishTime = System.nanoTime();
		double time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));
		
		// Second, query for nodes within circle!

		dataset.begin(ReadWrite.READ);
		startTime = System.nanoTime();
		
		Query q2 = null;
		QueryExecution qexec2 = null;		
		

		try {
			
			for(MyQueryResult qr: objList) {
				
				System.out.println(qr.lati.toString());
			    System.out.println(qr.longi.toString());
			    System.out.println(qr.checkin.toString());
			    System.out.println(qr.time.toString());	
			      
				// For each checkin-lat-long, query for near by node in db
				String qs2 = StrUtils.strjoinNL("SELECT DISTINCT ?node", "WHERE", "{", 
						"?node <" + PROP_GEO_LOC.getURI() + "> ?bn ;",
						"a <" + RES_NODE.getURI() + "> .",
						"?bn spatial:withinCircle (" + qr.lati.asLiteral().getFloat() + " " + qr.longi.asLiteral().getFloat() + " " + range + " 'km') .", 
						"}");
				
				qs2 = QUERY_PREFIX_MAIN + "\n" + qs2;
				System.out.println(qs2);
				
				q2 = QueryFactory.create(qs2);
				
				qexec2 = QueryExecutionFactory.create(q2, dataset);
				ResultSet nodeResults = qexec2.execSelect();
				//QueryExecUtils.executeQuery(q, qexec);

				for (; nodeResults.hasNext();) {
					QuerySolution nodeSoln = nodeResults.nextSolution();
					RDFNode node = nodeSoln.get("node");
					System.out.println(node.toString());

					// add each node to array list
					nodeList.add(node.toString());
				} // end each node withinCircle for
			
			} // end for-each MyQueryResult

		} finally {
			if(qexec2 != null) qexec2.close();
			dataset.end();
		}
		
		finishTime = System.nanoTime();
		time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));		

		return nodeList;
	}
	
	
	public void generateMashup() {
		// TODO map with tracking update in real-time, video always connect, latest status/info summary, archive log, near by user 
	}
	
	// IMP look more into genUUID() mechanism
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}
	
	private void checkInGps(String uid, String udid, String location) {
		// TODO checkinGPS
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
	public void checkInWifi(String uuid, String location, String macAddr) throws Exception {		

		// This either create or connect to existing dataset		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		// Jena createResource() will also get existing resource if already available
		// Note: ordered index is not used/generated here due to possible racing condition
		// which could break the true order of event, rather we rely on SEQ collection order	
		
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

		// Extract location's parameters from JSON
		GeoLocation geoLoc = new GeoLocation(gson.fromJson(location, TitaniumLocation.class));		
		// IMP can we get location name from wifi check-in in the future?
		
		// This will also create this wifi resource if not available before (Wardriving like)
		Resource wifi = createWifiResource(model, macAddr, geoLoc);
				
		Resource checkin = model.createResource(RES_CHK.getURI() + "." + generateUUID())
				.addProperty(PROP_VIA, wifi)
				.addLiteral(PROP_TIMESTAMP, System.currentTimeMillis())
				// IMP change to XSD date-time? but probably will lose precision of millisecond?				
				//.addLiteral(XSD.dateTime, ) 
				.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc));
		
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
	 * @return Resource of wifi router
	 */
	private Resource createWifiResource(Model model, String macAddr, GeoLocation geoLoc) {
		
		Resource wifi = model.createResource(RES_WIFI.getURI() + "." + macAddr);
		// check if this wifi router is already registered
		if(model.contains(wifi, PROP_MAC_ADDR, macAddr)) {			
			// if exists, just update wifi location as needed
			Statement lastUpdateStm = wifi.getProperty(PROP_LAST_UPDATE);	
			
			if(lastUpdateStm.getLong() < System.currentTimeMillis()) {
				lastUpdateStm.changeLiteralObject(System.currentTimeMillis());
				// remove old statements about location and add new one				
				model.remove(model.listStatements(
						new SimpleSelector(wifi.getProperty(PROP_GEO_LOC).getResource(), null, (RDFNode) null)
				));
				model.remove(wifi.getProperty(PROP_GEO_LOC));
				wifi.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc));
				//.addProperty(PROP_SSID, ssid)
			}
			
		} else {
			wifi.addProperty(PROP_MAC_ADDR, macAddr)
			.addProperty(PROP_GEO_LOC, createLocBnode(model, geoLoc))
			.addProperty(RDF.type, RES_NODE)
			//.addProperty(PROP_SSID, ssid)
			.addLiteral(PROP_LAST_UPDATE, System.currentTimeMillis());			
		}
		return wifi;
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
				// IMP may be source of error due to lost in precision double -> float
				.addLiteral(PROP_W3C_LAT, (float) geoLoc.getLatitude())
				.addLiteral(PROP_W3C_LONG, (float) geoLoc.getLongitude())
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
		// TODO this is for when we have access to router fw
		// update TDB for current user location at this wifi router
	}
	
	/**
	 * Map router's Mac Address (BSSID) with a geographical location
	 * @param macAddr Router's MAC address (BSSID) to get mapped location
	 * @param location the most updated location from the router itself (if no change leave blank or null)
	 */
	public void updateWifiLocation(String macAddr, String location) {
		
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
	 * any time with introduction of new RDF node.    
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
	 * IMP should separate local methods and API's methods 
	 * or consider using a Messaging Queue solution for RPC?
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
	

    private static Dataset initTDBDatasetWithLuceneSpatitalIndex(File indexDir, File TDBDir, boolean preserveOldData) throws IOException{
		SpatialQuery.init();
		if(!preserveOldData) {			
			deleteOldFiles(TDBDir);
			TDBDir.mkdir();		
			deleteOldFiles(indexDir);
			indexDir.mkdirs();			
		}
		return createDatasetByCode(indexDir, TDBDir);
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
     * createDatasetByCode
     * @param indexDir
     * @param TDBDir
     * @return
     * @throws IOException
     */
	private static Dataset createDatasetByCode(File indexDir, File TDBDir) throws IOException {
		// Base data
		Dataset ds1 = TDBFactory.createDataset(TDBDir.getAbsolutePath());
		return joinIndexDataset(indexDir, ds1);
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
		entDef.addSpatialPredicatePair(PROP_LAT, PROP_LONG);		
		
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
	
	private static void queryData(Dataset geoDs) throws IOException {
		
		long startTime;
		long finishTime;
		double time;
		
		String pre = QUERY_PREFIX_MAIN;

		String qs = "";
		
		System.out.println("withinCircle");
		startTime = System.nanoTime();
		qs = StrUtils.strjoinNL("SELECT * ",
				" { ?s spatial:withinCircle (37.78583 -122.40641 10.0 'km' 3) ."," }");

		geoDs.begin(ReadWrite.READ);
		try {
			Query q = QueryFactory.create(pre + "\n" + qs);
			QueryExecution qexec = QueryExecutionFactory.create(q, geoDs);
			QueryExecUtils.executeQuery(q, qexec);
		} finally {
			geoDs.end();
		}
		finishTime = System.nanoTime();
		time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));
		
		System.out.println("withinBox");
		startTime = System.nanoTime();
		qs = StrUtils.strjoinNL("SELECT * ",
				" { ?s spatial:withinBox (30.00 -130.00 40.00 -100.00) .", " }");
//				" { ?s spatial:withinBox (30.00 -100.00 40.00 -130.00 -1) ;",
//				"      rdfs:label ?label", " }");

		geoDs.begin(ReadWrite.READ);
		try {
			Query q = QueryFactory.create(pre + "\n" + qs);
			QueryExecution qexec = QueryExecutionFactory.create(q, geoDs);
			QueryExecUtils.executeQuery(q, qexec);
		} finally {
			geoDs.end();
		}
		finishTime = System.nanoTime();
		time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));		
		
		System.out.println("nearby");
		startTime = System.nanoTime();
		qs = StrUtils.strjoinNL("SELECT * ",
				" { ?s spatial:nearby (51.3000 -2.71000 100.0 'miles') ;",
				"      rdfs:label ?label", " }");
 
		geoDs.begin(ReadWrite.READ);
		try {
			Query q = QueryFactory.create(pre + "\n" + qs);
			QueryExecution qexec = QueryExecutionFactory.create(q, geoDs);
			QueryExecUtils.executeQuery(q, qexec);
		} finally {
			geoDs.end();
		}
		finishTime = System.nanoTime();
		time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));		

		
		// dump query
		/*
		System.out.println("dump");
		startTime = System.nanoTime();
		qs = StrUtils.strjoinNL("SELECT * ",
				" { ?s ?p ?o .", " }");

		geoDs.begin(ReadWrite.READ);
		try {
			Query q = QueryFactory.create(pre + "\n" + qs);
			QueryExecution qexec = QueryExecutionFactory.create(q, geoDs);
			QueryExecUtils.executeQuery(q, qexec);
		} finally {
			geoDs.end();
		}
		finishTime = System.nanoTime();
		time = (finishTime - startTime) / 1.0e6;
		log.info(String.format("FINISH - %.2fms", time));	
		*/		
		
		
	}
	
	public void addSamples() {
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();	
				
		//model = ModelFactory.createDefaultModel();
		Resource r = model.createResource(URI_BASE + "tokyo");

		model.addLiteral(r, PROP_W3C_LAT, 37.78583f);
		model.addLiteral(r, PROP_W3C_LONG, -122.40641f);
		
		Resource r2 = model.createResource(URI_BASE + "near.tokyo");

		model.addLiteral(r2, PROP_LAT, 37.000);
		model.addLiteral(r2, PROP_LONG, -122.000);			
				
		model.close();
		dataset.commit();
		dataset.end();
	
	}
	
	

}
