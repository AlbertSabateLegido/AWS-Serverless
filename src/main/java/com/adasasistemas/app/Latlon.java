package com.adasasistemas.app;

public class Latlon {
	
	private double latitude;
	private double longitude;
	
	private static double RATIO = 0.25;
	
	public Latlon(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public int generateIndex() {
		double x = roundOff(latitude/RATIO);
		double y = Math.abs(roundOff((longitude-90)/RATIO));
		return (int) (y*(360/RATIO) + x);
	}
	
	private double roundOff(double i) {
		if ((i - (int)i) > 0.5) return i = (int)i + 1;
		return i = (int)i;
	}
}