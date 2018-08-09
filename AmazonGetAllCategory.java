package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AmazonGetAllCategory {
	
	private static final String ACCESS_ID = "AKIAI7ZAR46RLH3KXPNQ";
	private static final String SECRET_KEY = "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";
	private static String CHILD_CAT_URL = "https://browsenodes.commercedna.com/amazon.in/browseNodeLookup/<objectId>.json?awsId="+ACCESS_ID+"&awsSecret="+SECRET_KEY;
	private static Executor executor;
	private static Executor executor2;
	private static final String filePath = "C:\\Users\\vksco\\Documents\\category\\";
	
	public static void main(String[] args) {
		executor = Executors.newFixedThreadPool(10);
		executor2 = Executors.newFixedThreadPool(10);
		String url = "https://browsenodes.commercedna.com/amazon.in/explore.json?awsId="+ACCESS_ID+"&awsSecret="+SECRET_KEY;
		try {
			JSONArray json = readJsonArrayFromUrl(url);
			
			json.forEach(obj -> {
				executor2.execute(new Runnable() {
					@Override
					public void run() {
						try {
							getAllChild((JSONObject) obj);
						} catch (JSONException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			});
			FileWriter fw = new FileWriter(new File(filePath+"main.json"));
			json.write(fw);
			fw.close();
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void getAllChild(JSONObject obj) throws JSONException, IOException {
	    final String id = obj.getString("BrowseNodeId");
		String url = CHILD_CAT_URL.replace("<objectId>", id);
		JSONObject json = readJsonObjectFromUrl(url);
		JSONArray childs = null;
		try{
			JSONObject ooobj = json.getJSONObject("Children");
			childs = ooobj.getJSONArray("BrowseNode");
		}catch(Exception e) {System.out.println("No Children of "+json);}
		if(childs==null || childs.length()<=0) {
			return;
		}
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					AmazonGetAllCategory.writeJSONToFile(json, filePath+id+".json");
				} catch (JSONException | IOException e) {
					e.printStackTrace();
				}
			}
		});
		childs.forEach(child -> {
			try {
				getAllChild((JSONObject) child);
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}
	
	public static void writeJSONToFile(JSONObject json, String filePath) throws JSONException, IOException {
		FileWriter fw = new FileWriter(new File(filePath));
		json.write(fw);
		fw.close();
	}

	private static JSONArray readJsonArrayFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONArray json = new JSONArray(jsonText);
			return json;
		} finally {
			is.close();
		}
	}
	
	private static JSONObject readJsonObjectFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}

}
