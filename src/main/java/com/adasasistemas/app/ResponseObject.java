package com.adasasistemas.app;

import java.util.ArrayList;
import java.util.List;

public class ResponseObject {
	String init;
	String bucket;
	Latlon location;
	List<Value> values;
	
	public ResponseObject() {
		values = new ArrayList<Value>();
	}
	
	public String getInit() {
		return init;
	}
	public void setInit(String init) {
		this.init = init;
	}
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public Latlon getLocation() {
		return location;
	}
	public void setLocation(Latlon location) {
		this.location = location;
	}
	public List<Value> getValues() {
		return values;
	}
	public void setValues(List<Value> values) {
		this.values = values;
	}
	
	
}
