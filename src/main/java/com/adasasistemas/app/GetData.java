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

public class GetData implements RequestHandler<RequestObject,List<ResponseObject>>
{	
	public static String BUCKET_NAME = "noaa-gfs-pds";
	public static int NTHREADS = 16;
	
	public List<ResponseObject> handleRequest(RequestObject request, Context context) {
		List<ResponseObject> response = new ArrayList<ResponseObject>();
		
		System.out.println(request.getParameter());
		System.out.println(request.getVertical_level());
		System.out.println(request.getDate());
		System.out.println(request.getHour());
		System.out.println(request.getLatitude());
		System.out.println(request.getLongitude());
		
		readS3Bucket(request,response);

		return response;
	}
	
	private void readS3Bucket(RequestObject request,List<ResponseObject> response) {
		AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME ,request.generateKey());
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        
        ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
        List<ReadS3ObjectRunnable> runnables = new ArrayList<ReadS3ObjectRunnable>();
        for (S3ObjectSummary os: objects) {
        	ReadS3ObjectRunnable task = new ReadS3ObjectRunnable(s3,os.getKey(),request.generateIndex());
	        runnables.add(task);
	        executor.execute(task);
        }
        executor.shutdown();
        try {
			executor.awaitTermination(1, TimeUnit.MINUTES);
			for(int i = 0; i < runnables.size(); ++i) {
				ResponseObject res = runnables.get(i).getResponse();
				response.add(res);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return;
	}
}