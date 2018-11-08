package com.adasasistemas.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class GetData implements RequestHandler<RequestObject,ResponseObject>
{	
	public static String BUCKET_NAME = "noaa-gfs-pds";
	public static int NTHREADS = 16;
	
	public ResponseObject handleRequest(RequestObject request, Context context) {
		ResponseObject response = new ResponseObject();
		
		System.out.println(request.getParameter());
		System.out.println(request.getVertical_level());
		System.out.println(request.getDate());
		System.out.println(request.getHour());
		System.out.println(request.getLatitude());
		System.out.println(request.getLongitude());
		
		readS3Bucket(request,response);

		return response;
	}
	
	private void readS3Bucket(RequestObject request,ResponseObject response) {
	    Latlon location = new Latlon(request.getLatitude(),request.getLongitude());
        response.setInit(request.getDate() + request.getHour().substring(0, 2));
        response.setLocation(location);
        
		AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME ,request.generateKey());
        List<S3ObjectSummary> objects = result.getObjectSummaries();

        List<ReadS3ObjectRunnable> runnables = new ArrayList<ReadS3ObjectRunnable>();
        ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
        for (S3ObjectSummary os: objects) {
        	ReadS3ObjectRunnable runnable = new ReadS3ObjectRunnable(s3,os.getKey(),location.generateIndex());
        	runnables.add(runnable);
        	executor.execute(runnable);
        }
        executor.shutdown();
        try {
			executor.awaitTermination(1, TimeUnit.MINUTES);
			for(int i = 0; i < runnables.size(); ++i) {
				Value res =  runnables.get(i).getValue();
				response.getValues().add(res);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return;
	}
}