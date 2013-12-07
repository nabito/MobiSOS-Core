package com.dadfha.mobisos;

import java.util.UUID;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;


public class MobiSosCore {
	
	private static final String TDB_DIR = "MobiSosDB/MainDataset";
	
	//@prefix n: <http://dadfha.com/ns/node#> .
	private static final String NS_MOBISOS = "http://www.dadfha.com/mobisos-rdf/1.0#";
	// Prefix must be a valid NCName - http://en.wikipedia.org/wiki/QName
	private static final String NS_PREFIX_MOBISOS = "mbs";
	// Resource naming prefix
	private static final String RES_PREFIX_USER = "user"; // for user	
	private static final String RES_PREFIX_REC = "records"; // for records
	private static final String RES_PREFIX_CHK = "chk"; // for check-in
	private static final String RES_PREFIX_WIFI = "wifi"; // for wifi
	
	
	private final Dataset dataset = TDBFactory.createDataset(TDB_DIR);
	private final Model model;
		
	public final Property PROP_HAS_CHECKIN;
	public final Property PROP_VIA;
	public final Property PROP_TIMESTAMP;
	public final Property PROP_GEO_LOC;
	public final Property PROP_UCODE;

	public final Property PROP_MAC_ADDR;
	public final Property PROP_LAST_UPDATE;
	public final Property PROP_LAST_SEEN;
	public final Property PROP_L10N_METHOD;
	
	// GeoLocation related properties
	public final Property PROP_ACCU;
	public final Property PROP_ALTI;
	public final Property PROP_ALTI_ACCU;
	public final Property PROP_HEADING;	
	public final Property PROP_LAT;
	public final Property PROP_LONG;
	public final Property PROP_SPEED;
	public final Property PROP_LOC_NAME;		
	
	private static Gson gson = new Gson();
			
	public MobiSosCore() {		
		
		// Init TDB (even if when already done before)				
		dataset.begin(ReadWrite.WRITE);
		model = dataset.getDefaultModel();
		model.setNsPrefix(NS_PREFIX_MOBISOS, NS_MOBISOS);
		
		PROP_HAS_CHECKIN = model.createProperty(NS_MOBISOS, "hasCheckin");
		PROP_VIA = model.createProperty(NS_MOBISOS, "via");
		PROP_TIMESTAMP = model.createProperty(NS_MOBISOS, "timestamp");
		PROP_GEO_LOC = model.createProperty(NS_MOBISOS, "geoLocation");		
		PROP_UCODE = model.createProperty(NS_MOBISOS, "ucode");
		
		PROP_MAC_ADDR = model.createProperty(NS_MOBISOS, "macAddress");
		PROP_LAST_UPDATE = model.createProperty(NS_MOBISOS, "lastUpdate");
		PROP_LAST_SEEN = model.createProperty(NS_MOBISOS, "lastSeen");		
		PROP_L10N_METHOD = model.createProperty(NS_MOBISOS, "l10nMethod");
		
		PROP_ACCU = model.createProperty(NS_MOBISOS, "accuracy");
		PROP_ALTI = model.createProperty(NS_MOBISOS, "altitude");
		PROP_ALTI_ACCU = model.createProperty(NS_MOBISOS, "altitudeAccuracy");
		PROP_HEADING = model.createProperty(NS_MOBISOS, "heading");
		PROP_LAT = model.createProperty(NS_MOBISOS, "latitude");
		PROP_LONG = model.createProperty(NS_MOBISOS, "longitude");		
		PROP_SPEED = model.createProperty(NS_MOBISOS, "speed");
		PROP_LOC_NAME = model.createProperty(NS_MOBISOS, "locationName");
		
		dataset.end();		
		
	}
	
	
	public void generateMashup() {
		
	}
	
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}

	/**
	 * This method is expected to be called from a mobile client to do self check-in
	 * since Wifi router doesn't know about uid 
	 * @param uid user ID
	 * @param location the check-in location in JSON. Structure follow that of Titanium3.x.
	 * @param macAddr router's MAC address (BSSID)
	 */
	public void checkInAtWifi(String uid, String location, String macAddr) {		

		// This either create or connect to existing dataset		
		dataset.begin(ReadWrite.WRITE);
		
		// This will also get existing resource if already available
		// IMP look more into genUUID() mechanism
		// Note: ordered index is not used/generated here due to possible racing condition
		// which could break the true order of event, rather we rely on SEQ collection order	
		// FIXME Be warn that a new user can be created with this method, need prior checking!
		Resource user = model.createResource(NS_MOBISOS + RES_PREFIX_USER + uid);
		
		// Get check-in records (SEQ) of the user
		Seq chkRecords = user.getProperty(PROP_HAS_CHECKIN).getSeq(); 
		if(chkRecords == null) throw new RuntimeException("Possibly Data Error! This user still has no check-in record yet!");
		
		// This will also create this wifi resource if not available before (Wardriving like)
		Resource wifi = model.createResource(NS_MOBISOS + RES_PREFIX_WIFI + macAddr);

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
		
		Resource checkin = model.createResource(NS_PREFIX_MOBISOS + RES_PREFIX_CHK + generateUUID())
				.addProperty(PROP_VIA, wifi)
				.addLiteral(PROP_TIMESTAMP, System.currentTimeMillis())
				.addProperty(PROP_GEO_LOC, locBnode);
		
		chkRecords.add(checkin);		
		
		dataset.end();

	}
	
	/**
	 * This method is expected to be called from a Wifi router to track near by/connected user
	 * by sending user's MAC address to 
	 * @param devUUID user device's MAC address for Android and app's UUID for iOS >= 7 
	 * @param macAddr Router's MAC address (BSSID) to get mapped location
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
	 * @param email
	 * @param passwd
	 * @return
	 */
	public boolean createUser(String email, String passwd) {
		boolean result = false;
		// IMP include password someday soon?
		String uid = generateUUID();
		Resource user = model.createResource(NS_MOBISOS + RES_PREFIX_USER + uid)
				.addProperty(PROP_HAS_CHECKIN, model.createSeq(NS_MOBISOS + RES_PREFIX_REC + uid))
				.addProperty(FOAF.mbox, email);
		
		return (user != null)? true:false;
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
	

}
