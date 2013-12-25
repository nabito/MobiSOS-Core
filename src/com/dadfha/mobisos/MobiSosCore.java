package com.dadfha.mobisos;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiCmd;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.sparql.util.QueryExecUtils;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

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
	
	// Resource naming prefix
	private static final String RES_PREFIX_USER = "user"; // for user	
	private static final String RES_PREFIX_REC = "records"; // for records
	private static final String RES_PREFIX_CHK = "chk"; // for check-in
	private static final String RES_PREFIX_WIFI = "wifi"; // for wifi
	
	private Dataset dataset;
	private Model model;
	
	//public static final Resource RES_XXX  = resource( "Xxx");	
	
	// IMP consider adding rdfs:label and rdfs:comment for each of property
	// so it's become sentences and can be saved to TDB
	public static final Property PROP_UUID = property("uuid");
	public static final Property PROP_HAS_CHECKIN = property("hasCheckin");
	public static final Property PROP_VIA = property("via");
	public static final Property PROP_TIMESTAMP = property("timestamp");
	public static final Property PROP_GEO_LOC = property("geolocation");
	public static final Property PROP_UCODE = property("ucode");

	public static final Property PROP_MAC_ADDR = property("macAddress");
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
	
	public static final Property W3C_LAT = property(URI_W3C_GEO, "lat");
	public static final Property W3C_LONG = property(URI_W3C_GEO, "long");
	
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
		
		dataset = initTDBDatasetWithLuceneSpatitalIndex(TDB_INDEX_DIR, TDB_DIR, true);

		//loadData(geoDataset, "/Users/Wirawit/Desktop/geoarq-data-1.ttl");		
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		// IMP consider naming graph for better management/performance?
		// dataset.addNamedModel(NS_MOBISOS + NS_PREFIX_MOBISOS, model);
		model.setNsPrefix(URI_PREFIX_BASE, URI_BASE);
		model.setNsPrefix(URI_PREFIX_W3C_GEO, URI_W3C_GEO);
				
		/*
		//model = ModelFactory.createDefaultModel();
		Resource r = model.createResource(NS_MOBISOS + "root");
		Property p1 = model.createProperty(NS_MOBISOS + "nama");
		Property p2 = model.createProperty(NS_MOBISOS, "vaja");
		model.add(r, p1, "arto");
		model.add(r, p2, "Pairorrrr");
		model.add(p1, RDFS.label, "The Nama");
		model.add(p2, RDFS.label, "The Vaja");
		//model.write(System.out, "Turtle");
		 */

		model.close();		
		dataset.commit();
		dataset.end();
		
		
	}
	
	public void sosCall(String uid, String location) throws IOException {
		
		System.out.println("SOS called on mobiSOS-Core");
		
		boolean success = true;
		
		// TODO update latest checkin to DB, and turn user's SOS flag on
		
		
		// query semantic DB for nearby nodes
		queryData(dataset);
		
		if(!success) throw new RuntimeException("Can't complete sos call operation.");
		
		
		// then ask for tracking record "Do you see Bob?"		
	}
	
	
	public void generateMashup() {
		// TODO map with tracking update in real-time, video always connect, latest status/info summary, archive log, near by user 
	}
	
	// IMP look more into genUUID() mechanism
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}

	/**
	 * This method is expected to be called from a mobile client to do self check-in
	 * since Wifi router doesn't know about uid 
	 * 
	 * IMP password/API token must be accompany so any person has uuid won't be able to disguise check-in
	 * 
	 * @param uid user ID
	 * @param location the check-in location in JSON. Structure follow that of Titanium3.x 
	 * http://docs.appcelerator.com/titanium/latest/#!/api/LocationCoordinates
	 * @param macAddr router's MAC address (BSSID) in Hexadecimal without ':'
	 * @return true when success, false otherwise
	 */
	public boolean checkInWifi(String uid, String location, String macAddr) {		

		// This either create or connect to existing dataset		
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		// Jena createResource() will also get existing resource if already available
		// Note: ordered index is not used/generated here due to possible racing condition
		// which could break the true order of event, rather we rely on SEQ collection order	
		
		Resource user = model.createResource(URI_BASE + RES_PREFIX_USER + uid);
		
		// Only allow authorized user to check-in
		//if(!model.listResourcesWithProperty(PROP_UUID, uid).hasNext()) {
		if(!model.contains(user, PROP_UUID, uid)) {
			System.out.println("Trap!");
			dataset.abort();
			model.close();
			dataset.end();			
			return false;
		}
		
		// Get check-in records (SEQ) of the user
		Seq chkRecords = user.getProperty(PROP_HAS_CHECKIN).getSeq(); 
		if(chkRecords == null) throw new RuntimeException("Possibly Data Error! This user still has no check-in record yet!");
		
		// This will also create this wifi resource if not available before (Wardriving like)
		Resource wifi = model.createResource(URI_BASE + RES_PREFIX_WIFI + macAddr);

		// Extract location's parameters from JSON and put it in bnode
		GeoLocation geoLoc = new GeoLocation(gson.fromJson(location, TitaniumLocation.class));
		Resource locBnode = model.createResource();
		locBnode.addLiteral(PROP_ACCU, geoLoc.getAccuracy())
				.addLiteral(PROP_ALTI, geoLoc.getAltitude())
				.addLiteral(PROP_ALTI_ACCU, geoLoc.getAltitudeAccuracy())
				.addLiteral(PROP_HEADING, geoLoc.getHeading())
				.addLiteral(PROP_LAT, geoLoc.getLatitude())
				.addLiteral(PROP_LONG, geoLoc.getLongitude())
				.addLiteral(PROP_SPEED, geoLoc.getSpeed())
				.addLiteral(PROP_LOC_NAME, ""); // IMP can we get location name from wifi check-in in the future?
		
		Resource checkin = model.createResource(URI_BASE + RES_PREFIX_CHK + generateUUID())
				.addProperty(PROP_VIA, wifi)
				.addLiteral(PROP_TIMESTAMP, System.currentTimeMillis())
				.addProperty(PROP_GEO_LOC, locBnode);
		
		chkRecords.add(checkin);		
		
		model.close();
		dataset.commit();
		dataset.end();
		
		return true;

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
	 * anytime with introduction of new RDF node.    
	 * 
	 * @param email
	 * @param passwd
	 * @return String unique user id
	 */
	public String createUser(String email, String passwd) {
		// TODO include password someday soon?
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		
		String uid = generateUUID();
		Resource user = model.createResource(URI_BASE + RES_PREFIX_USER + uid)
				.addProperty(PROP_HAS_CHECKIN, model.createSeq(URI_BASE + RES_PREFIX_REC + uid))
				.addProperty(FOAF.mbox, email)
				.addProperty(PROP_UUID, uid);	
		
		model.close();
		dataset.commit();
		dataset.end();
		return uid;		
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
	

    private static Dataset initTDBDatasetWithLuceneSpatitalIndex(File indexDir, File TDBDir, boolean preserveTdbData) throws IOException{
		SpatialQuery.init();
		deleteOldFiles(indexDir);
		indexDir.mkdirs();
		if(!preserveTdbData) {
			deleteOldFiles(TDBDir);
			TDBDir.mkdir();			
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
		entDef.addWKTPredicate(ResourceFactory.createResource("http://localhost/jena_example/#wkt_1"));
		
		// Lucene, index in File system.
		Directory dir = FSDirectory.open(indexDir);

		// Join together into a dataset
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
		
		String pre = StrUtils.strjoinNL("PREFIX " + URI_PREFIX_BASE + ": <" + URI_BASE + ">",
				"PREFIX " + URI_PREFIX_JENA_SPATIAL + ": <" + URI_JENA_SPATIAL + ">",
				"PREFIX " + URI_PREFIX_RDFS + ": <"+ RDFS.getURI() +">");
		
		/*
		String precheck = StrUtils.strjoinNL("PREFIX mbs: <http://www.dadfha.com/mobisos-rdf/1.0#>",
				"PREFIX spatial: <http://jena.apache.org/spatial#>",
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");		
		
		if(pre.equals(precheck)) System.out.println("Yay"); else System.out.println("Boo"); 
		*/
		
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
	/*
	public void addSamples() {
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
				
		//model = ModelFactory.createDefaultModel();
		Resource r = model.createResource(URI_BASE + "tokyo");

		model.addLiteral(r, W3C_LAT, 37.78583f);
		model.addLiteral(r, W3C_LONG, -122.40641f);	
				
		model.close();
		dataset.commit();
		dataset.end();
	
	}
	*/
	

}
