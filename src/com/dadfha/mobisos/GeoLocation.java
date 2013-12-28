package com.dadfha.mobisos;
/**
 * compatible with Titanium Mobile FW location object
 * http://docs.appcelerator.com/titanium/3.0/#!/api/LocationCoordinates  
 * @author Wirawit
 *
 */
public class GeoLocation extends TitaniumLocation {
	
	private String locationName = "";
	
	public GeoLocation() {
		
	}
	
	public GeoLocation(TitaniumLocation tl) {
		super(tl);		
	}

	/**
	 * @return the locationName
	 */
	public String getLocationName() {
		return locationName;
	}

	/**
	 * @param locationName the locationName to set
	 */
	public void setLocationName(String locationName) {
		this.locationName = locationName;
	}

}
