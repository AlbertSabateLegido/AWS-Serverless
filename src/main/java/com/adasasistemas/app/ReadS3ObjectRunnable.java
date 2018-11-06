package com.adasasistemas.app;

import java.io.IOException;

import org.json.JSONObject;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import ucar.nc2.grib.grib2.Grib2Record;
import ucar.nc2.grib.grib2.Grib2RecordScanner;
import ucar.unidata.io.http.HTTPRandomAccessFile;

public class ReadS3ObjectRunnable implements Runnable {
	
	private AmazonS3 s3;
	private S3ObjectSummary object;
	private int index;
	
	private JSONObject result;
	
	public ReadS3ObjectRunnable(AmazonS3 s3,S3ObjectSummary object,int index) {
		this.s3 = s3;
		this.object = object;
		this.index = index;
		result = new JSONObject();
	}
	
	public JSONObject getResult() {
		return result;
	}

	public void run() {
		try {
			String url = s3.getUrl(App.BUCKET_NAME,object.getKey()).toString();
			HTTPRandomAccessFile httpFile = new HTTPRandomAccessFile(url);
			Grib2RecordScanner scanner = new Grib2RecordScanner(httpFile);
			while(scanner.hasNext()) {
				Grib2Record record = scanner.next();
							  
				float[] data = record.readData(httpFile);
				System.out.println("Data for " + object.getKey() + ": " + data[index]);
				result.put("key", object.getKey());
				result.put("data", data[index]);
			
				httpFile.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
