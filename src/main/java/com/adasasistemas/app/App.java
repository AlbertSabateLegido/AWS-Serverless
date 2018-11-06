package com.adasasistemas.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App 
{	
	public static String BUCKET_NAME = "noaa-gfs-pds";
	public static int NTHREADS = 16;
	
	public Map<String,Object> myHandler(Map<String,Object> header, Context context) {
		Map<String,Object> parameters = new HashMap<String,Object>();
		if(header.containsKey("body")) {
			parameters = getParameters((String) header.get("body"));
		}
		
		//ERROR CONTROL
		if(parameters.size() != 3) {
			Map<String,Object> output = new HashMap<String,Object>();
			output.put("statusCode", 502);
			output.put("body", "Parameters should be 'key:{my-key},latitude:{my-latitude},longitude:{my-longitude}'");
			return output;
		}
		
		String key = (String) parameters.get("key");
		double lat = Double.parseDouble((String) parameters.get("latitude"));
		double lon = Double.parseDouble((String) parameters.get("longitude"));
		
		String body = readS3Bucket(key,new Latlon(lat,lon).getIndex()).toString();
		
		Map<String,Object> output = new HashMap<String,Object>();
		output.put("statusCode", 200);
		output.put("body", body);

		return output;
	}
	
	private JSONObject readS3Bucket(String key,int index) {
		AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME ,key);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        
        ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
        List<ReadS3ObjectRunnable> runnables = new ArrayList<ReadS3ObjectRunnable>();
        for (S3ObjectSummary os: objects) {
        	ReadS3ObjectRunnable task = new ReadS3ObjectRunnable(s3,os,index);
	        runnables.add(task);
	        executor.execute(task);
        }
        executor.shutdown();
        JSONObject response = new JSONObject();
        try {
			executor.awaitTermination(1, TimeUnit.MINUTES);
			for(int i = 0; i < runnables.size(); ++i) {
				response.putOnce(String.valueOf(i), runnables.get(i).getResult());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return response;
	}
	
	private Map<String,Object> getParameters(String httpRequest) {
		Map<String,Object> parameters = new HashMap<String,Object>();
        for(String mapEntry:httpRequest.split(",")) {
        	String[] pairKeyValue = mapEntry.split(":");
  
        	if(pairKeyValue.length == 2 && (pairKeyValue[0].equals("key") 
        			|| pairKeyValue[0].equals("latitude") || pairKeyValue[0].equals("longitude")))
        		parameters.put(pairKeyValue[0],pairKeyValue[1]);
        }
		return parameters;
	}
	
	/* MAIN FUNCTION (Just for testing)*/
    public static void main( String[] args )
    {
        String key = "TMPK/2 m HGHT/20181031/0000/";
        
        double latitude  = 101.9;
        double longitude = 12.9;
        
        int index = new Latlon(latitude,longitude).getIndex();
        
        String header = "nThreads:16,lat:10,lon:11";
        Map<String,Object> input = new HashMap<String,Object>();
        for(String keyValue:header.split(",")) {
        	String[] pair = keyValue.split(":");
        	input.put(pair[0],pair[1]);
        }
        
        System.out.println(input.toString());
        
        long initialTimer = System.currentTimeMillis();
           
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME ,key);
        
        
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        long timer1 = System.currentTimeMillis();
        System.out.println("First call: " + (timer1 - initialTimer));
        System.out.println("Get Files");
        
        ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
        List<ReadS3ObjectRunnable> runnables = new ArrayList<ReadS3ObjectRunnable>();
        for (S3ObjectSummary os: objects) {
        	ReadS3ObjectRunnable task = new ReadS3ObjectRunnable(s3,os,index);
        	runnables.add(task);
	        executor.execute(task);
        }
        try {
        	executor.shutdown();
			executor.awaitTermination(1, TimeUnit.MINUTES);
			System.out.println("TIME: " + (System.currentTimeMillis() - initialTimer));
			JSONObject response = new JSONObject();
			int i = 0;
			for(ReadS3ObjectRunnable runnable:runnables) {
				response.putOnce(String.valueOf(i), runnable.getResult());
				++i;
			}
			System.out.println(response);
			System.out.println("TIME: " + (System.currentTimeMillis() - initialTimer));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}