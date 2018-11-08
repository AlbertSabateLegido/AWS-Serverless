package com.adasasistemas.app;

public class RequestObject {
	String parameter;
	String vertical_level;
	//YYYYMMDD
	String date;
	//HH00
	String hour;
	double latitude;
	double longitude;
	
	public String getParameter() {
		return parameter;
	}
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	public String getVertical_level() {
		return vertical_level;
	}
	public void setVertical_level(String vertical_level) {
		this.vertical_level = vertical_level;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getHour() {
		return hour;
	}
	public void setHour(String hour) {
		this.hour = hour;
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
	public String generateKey() {
		return parameter + '/' + vertical_level + '/' + date + '/' + hour + '/';
	}
	public int generateIndex() {
		return new LatLon(latitude,longitude).generateIndex();
	}
	
}
