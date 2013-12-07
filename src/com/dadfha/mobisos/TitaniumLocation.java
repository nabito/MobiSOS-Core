package com.dadfha.mobisos;

public class TitaniumLocation {

	private double accuracy;	// Accuracy of the location update, in meters.
	private double altitude;	// Altitude of the location update, in meters.
	private double altitudeAccuracy;	// Vertical accuracy of the location update, in meters.
	private double heading;		// Compass heading, in degrees. ...
	private double latitude;	// Latitude of the location update, in decimal degrees.
	private double longitude;	// Longitude of the location update, in decimal degrees.
	private double speed;		// Current speed in meters/second. ...
	private long timestamp = System.currentTimeMillis();	// Timestamp for this location update, in milliseconds.
	
	public TitaniumLocation() {
		
	}
	
	public TitaniumLocation(TitaniumLocation tl) {
		this.accuracy = tl.accuracy;
		this.altitude = tl.altitude;
		this.altitudeAccuracy = tl.altitudeAccuracy;
		this.heading = tl.heading;
		this.latitude = tl.latitude;
		this.longitude = tl.longitude;
		this.speed = tl.speed;
		this.timestamp = tl.timestamp;
	}

	/**
	 * @return the accuracy
	 */
	public double getAccuracy() {
		return accuracy;
	}

	/**
	 * @param accuracy the accuracy to set
	 */
	public void setAccuracy(double accuracy) {
		this.accuracy = accuracy;
	}

	/**
	 * @return the altitude
	 */
	public double getAltitude() {
		return altitude;
	}

	/**
	 * @param altitude the altitude to set
	 */
	public void setAltitude(double altitude) {
		this.altitude = altitude;
	}

	/**
	 * @return the altitudeAccuracy
	 */
	public double getAltitudeAccuracy() {
		return altitudeAccuracy;
	}

	/**
	 * @param altitudeAccuracy the altitudeAccuracy to set
	 */
	public void setAltitudeAccuracy(double altitudeAccuracy) {
		this.altitudeAccuracy = altitudeAccuracy;
	}

	/**
	 * @return the heading
	 */
	public double getHeading() {
		return heading;
	}

	/**
	 * @param heading the heading to set
	 */
	public void setHeading(double heading) {
		this.heading = heading;
	}

	/**
	 * @return the latitude
	 */
	public double getLatitude() {
		return latitude;
	}

	/**
	 * @param latitude the latitude to set
	 */
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	/**
	 * @return the longitude
	 */
	public double getLongitude() {
		return longitude;
	}

	/**
	 * @param longitude the longitude to set
	 */
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	/**
	 * @return the speed
	 */
	public double getSpeed() {
		return speed;
	}

	/**
	 * @param speed the speed to set
	 */
	public void setSpeed(double speed) {
		this.speed = speed;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}	
	
}
