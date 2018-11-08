package com.adasasistemas.app;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.services.s3.AmazonS3;

import ucar.nc2.grib.grib2.Grib2Record;
import ucar.nc2.grib.grib2.Grib2RecordScanner;
import ucar.unidata.io.http.HTTPRandomAccessFile;

public class ReadS3ObjectRunnable implements Runnable {
	
	private AmazonS3 s3;
	private String key;
	private int index;
	
	private Value value;
	
	public ReadS3ObjectRunnable(AmazonS3 s3,String key,int index) {
		this.s3 = s3;
		this.key = key;
		this.index = index;
		this.value = new Value();
	}
	
	public Value getValue() {
		return value;
	}
	
	public void run() {
		try {
			String url = s3.getUrl(GetData.BUCKET_NAME,key).toString();
			HTTPRandomAccessFile httpFile = new HTTPRandomAccessFile(url);
			Grib2RecordScanner scanner = new Grib2RecordScanner(httpFile);
			while(scanner.hasNext()) {
				Grib2Record record = scanner.next();
				float[] data = record.readData(httpFile);
				value.setValue(data[index]);
				Date date = getDate(key);
				value.setDate(getDate(date));
				value.setHour(getHour(date));
				value.setTime(getTime(key));
				httpFile.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} 
	}
	
	private String getTime(String key) {
		String[] chunks = key.split("/");
		return chunks[chunks.length-1];
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
	
	private String getDate(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(date);
	}
	
	private String getHour(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		return sdf.format(date);
	}
}
