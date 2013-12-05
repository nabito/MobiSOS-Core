package com.dadfha.mobisos;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;

public class MobiSosCore {
	
	private static Gson gson = new Gson();
	
	
	public void createReport() {
		
	}

	public void trackWifi(String location) {

		long timestamp = System.currentTimeMillis();

		String directory = "MobiSosDB/MainDataset";
		Dataset dataset = TDBFactory.createDataset(directory);

		dataset.begin(ReadWrite.WRITE);
		Model model = dataset.getDefaultModel();
		model.add(new Resource(), new Predicate(), new Resource());
		dataset.end();

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
	
	public boolean createUser() {
		boolean result = false;
		// TODO
		return result;
	}
	
	public String getUserById(String id) {
		
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
