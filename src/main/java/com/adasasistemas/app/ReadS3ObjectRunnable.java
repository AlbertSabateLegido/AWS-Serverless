package com.adasasistemas.app;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
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
				result.put("key", object.getKey());
				result.put("data", data[index]);
				result.put("date", getDate(object.getKey()));
				httpFile.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	//key must follow the model "Parameter/Vertical Level/YYYYMMDD/HH00/FFF"
	private Date getDate(String key) throws ParseException {
		String[] chunks = key.split("/");
		Date date = new Date();
		if(chunks.length == 5) {
			String stringDate  = chunks[2];
			int hour = Integer.parseInt(chunks[3].substring(0, 2)) + Integer.parseInt(chunks[4]);
			String stringHour = new String();
			if(hour < 10) stringHour = "00" + String.valueOf(hour);
			else if(hour < 100) stringHour = "0" + String.valueOf(hour);
			else stringHour = String.valueOf(hour);
			stringDate += stringHour;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHH");
			date = sdf.parse(stringDate);
		}
		return date;
	}
}
