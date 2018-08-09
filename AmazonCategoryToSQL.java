package com.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AmazonCategoryToSQL {
	
	private final static String AMAZON_CATEGORY_PATH = "C:\\Users\\vksco\\Documents\\category\\";
	private static BufferedWriter bw;
	private static ExecutorService executor;
	private static volatile int i = 0;

	public static void main(String[] args) throws IOException {
		bw = new BufferedWriter(new FileWriter(AMAZON_CATEGORY_PATH+"main.sql"));
		executor = Executors.newFixedThreadPool(20);
		createSQLInsertCommand();
		try {
			JSONArray jsonArray  = readJsonArrayFromUrl(AMAZON_CATEGORY_PATH+"main.json");
			jsonArray.forEach(tmp -> {
				final JSONObject obj = (JSONObject) tmp;
				executor.execute(new Runnable() {
					@Override
					public void run() {
						int parentId = AmazonCategoryToSQL.writeToAFile(obj, null);
						getAllChild(obj,parentId);
					}
				});
			});
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			System.out.println("Executor Terminated");
			bw.flush();
			bw.close();
		} catch (InterruptedException e) {
		  System.out.println();
		}
	}
	
	private static void getAllChild(JSONObject obj, Integer parentId) {
		String id = obj.getString("BrowseNodeId");
		JSONObject jsonObject = null;
		try {
			jsonObject = readJsonObjectFromUrl(AMAZON_CATEGORY_PATH+id+".json");
		} catch (JSONException | IOException e) {
			System.out.println("File Not Found for : "+id);
		}
		if(jsonObject==null) {
			return;
		}
		JSONArray childs = null;
		try{
			JSONObject ooobj = jsonObject.getJSONObject("Children");
			childs = ooobj.getJSONArray("BrowseNode");
		}catch(Exception e) {System.out.println("No Children of "+jsonObject);}
		if(childs==null || childs.length()<=0) {
			return;
		}
		childs.forEach(child -> {
			try {
				int newparentId = AmazonCategoryToSQL.writeToAFile((JSONObject) child, parentId);
				getAllChild((JSONObject) child, newparentId);
			} catch (JSONException e) {
				e.printStackTrace();
			} 
		});
	}
	
	private static synchronized int writeToAFile(JSONObject obj,Integer onj) {
		i+=1;
		String name = obj.getString("Name").replaceAll("[']", "''");
		//String id = obj.getString("BrowseNodeId");
		String search_index = name.replaceAll("[&',]", " ")
				.replace("and", " ")
				.replaceAll("[\\s]+", " ")
				.toLowerCase();
		String code = i + "_" + name.replaceAll("[\\s']", "_").toLowerCase();;
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		sb.append(i).append(",");
		sb.append("'").append(code).append("'").append(",");
		sb.append("'").append(name).append("'").append(",");
		sb.append("'").append(name).append("'").append(",");
		sb.append(0).append(",");
		sb.append(1).append(",");
		sb.append("'").append(search_index).append("'").append(",");
		sb.append(onj).append("),");
		sb.append(System.lineSeparator());
		try {
			bw.write(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return i;
	}
	
	private static void createSQLInsertCommand() {
		try {
			bw.write("INSERT INTO category (id, code, name, description, is_system, is_active, search_index, parent_id) VALUES");
			bw.write(System.lineSeparator());
		} catch (IOException e) {
			try {
				bw.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private static JSONArray readJsonArrayFromUrl(String url) throws IOException, JSONException {
		InputStream is = new FileInputStream(url);
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
		InputStream is = new FileInputStream(url);
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}
	
	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		bw.flush();
		bw.close();
	}
}
