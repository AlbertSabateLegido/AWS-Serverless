package com.adasasistemas.app;

public class Latlon {
	
	private double latitude;
	private double longitude;
	
	private static double RATIO = 0.25;
	
	public Latlon(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public int getIndex() {
		double x = roundOff(latitude/RATIO);
		//System.out.println("X " + x);
		double y = Math.abs(roundOff((longitude-90)/RATIO));
		//System.out.println("Y " + y);
		return (int) (y*(360/RATIO) + x);
	}
	
	private double roundOff(double i) {
		if ((i - (int)i) > 0.5) return i = (int)i + 1;
		return i = (int)i;
	}
}