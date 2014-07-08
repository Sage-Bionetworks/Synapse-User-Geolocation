package org.sagebionetworks;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.SynapseProfileProxy;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserProfile;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * Hello world!
 *
 */
public class SynapseUserGeolocation {
	private static final int PAGE_SIZE = 50;
	private static final String BUCKET_NAME = "geoloc.sagebase.org";
	    private static final String JS_FILE_NAME = "geoLocate.js";
    private static final String MAIN_PAGE_FILE_TEMPLATE = "indexTemplate.html";
    private static final String MAIN_PAGE_FILE_NAME = "index.html";
    // this allows us to test without processing all users
    private static final int MAX_GEO_POSNS = 20; //10000;
   
	
    public static void main( String[] args ) throws Exception {
    	geoLocate();
    }
    
    public static void geoLocate() throws Exception {
    	String synapseUserName = getProperty("SYNAPSE_USERNAME");
    	String synapsePassword = getProperty("SYNAPSE_PASSWORD");
        SynapseClient synapseClient = createSynapseClient();
        synapseClient.login(synapseUserName, synapsePassword);
    	long total = Integer.MAX_VALUE;
    	int latLngCount = 0;
    	int geoLocatedUsersCount = 0;
    	Map<String,JSONObject> geoLocMap = new HashMap<String,JSONObject>();
       	for (int offset=0; offset<total && latLngCount<MAX_GEO_POSNS; offset+=PAGE_SIZE) {
       		PaginatedResults<UserProfile> pr = synapseClient.getUsers(offset, PAGE_SIZE);
        	total = (int)pr.getTotalNumberOfResults();
        	List<UserProfile> page = pr.getResults();
        	for (int i=0; i<page.size() && latLngCount<MAX_GEO_POSNS; i++) {
        		UserProfile up = page.get(i);
        		if (!empty(up.getLocation())) {
        			String fixedLocation = up.getLocation().
        					replaceAll("Bogotï¿½", "Bogotá").
        					replaceAll("Dï¿½sseldorf", "Düsseldorf").
        					replaceAll("Quï¿½bec", "Québec").
        					replaceAll("Santo Andrï¿½", "Santo André").
        					replaceAll("Tï¿½bingen", "Tübingen").
        					replaceAll("Zï¿½rich", "Zürich");
        			JSONObject geoLocatedInfo = geoLocMap.get(fixedLocation);

        			if (geoLocatedInfo==null) {
        				String encodedLocation = URLEncoder.encode(fixedLocation, "utf-8");
        				String urlString = "https://maps.googleapis.com/maps/api/geocode/json?sensor=true_or_false&address="+
        						encodedLocation;
        				String json = executeJsonQuery(urlString);
        				Thread.sleep(500L);
        				double[] latlng = getLatLngFromResponse(json);
        				if (latlng==null) {
        					if (fixedLocation.startsWith("Greater ") && fixedLocation.endsWith(" Area")) {
        						String upString = fixedLocation;
        						upString = upString.substring(8);
        	        			upString = upString.substring(0, upString.length()-5).trim();
                				encodedLocation = URLEncoder.encode(upString, "utf-8");
                				urlString = "https://maps.googleapis.com/maps/api/geocode/json?sensor=true_or_false&address="+
                						encodedLocation;
                				json = executeJsonQuery(urlString);
                				Thread.sleep(500L);
                				latlng = getLatLngFromResponse(json);
        					}
        					if (latlng==null) System.out.println("No result for "+urlString);
        				}
        				if (latlng!=null) {
        					// TODO if there is an existing lat/lng that matches this one, then merge
        					geoLocatedInfo = new JSONObject();
        					geoLocatedInfo.put("location", fixedLocation);
        					JSONArray jsonLatLng = new JSONArray();
        					for (int ll=0; ll<2; ll++) jsonLatLng.put(ll, latlng[ll]);
        					geoLocatedInfo.put("latLng", jsonLatLng);
        					JSONArray userIds = new JSONArray();
        					userIds.put(userIds.length(), up.getOwnerId());
        					geoLocatedInfo.put("userIds", userIds);
        					geoLocMap.put(fixedLocation, geoLocatedInfo);
        					geoLocatedUsersCount++;
        				}
        			} else {
        				JSONArray userIds = (JSONArray)geoLocatedInfo.get("userIds");
        				userIds.put(userIds.length(), up.getOwnerId());
    					geoLocatedUsersCount++;
        			}
        			latLngCount++;
        		}
        	}
       	}
    	JSONArray allInfo = new JSONArray();
    	for (JSONObject gli : geoLocMap.values()) {
			allInfo.put(allInfo.length(), gli);
    		
    	}
    	System.out.println("Number of geolocated users: "+geoLocatedUsersCount+".  Number of distinct locations: "+geoLocMap.size());
    	//System.out.println(allInfo);
    	
    	// upload the js file
    	String jsContent = readTemplate(JS_FILE_NAME, null);
    	uploadFile(JS_FILE_NAME, jsContent);
    	// upload the main page
    	Map<String,String> fieldValues = new HashMap<String,String>();
    	fieldValues.put("##geoLocInfo##", allInfo.toString());
    	fieldValues.put("##numberOfUsers##", ""+geoLocatedUsersCount);
    	// TODO: add hyperlinks to Team pages
    	String mainPageContent = readTemplate(MAIN_PAGE_FILE_TEMPLATE, fieldValues);
    	uploadFile(MAIN_PAGE_FILE_NAME, mainPageContent);
    	// TODO:  add Team pages
    	System.out.println("Finished uploading files to S3.  Visit http://s3.amazonaws.com/"+BUCKET_NAME+"/"+MAIN_PAGE_FILE_NAME);
    }
    
    private static boolean empty(String s) {
    	return s==null || s.length()==0;
    }
    
	private static String executeJsonQuery(String urlString) throws IOException {
		URL url = new URL(urlString);
		URLConnection conn = url.openConnection();
		conn.addRequestProperty("Accept", "application/json");
		InputStream is = conn.getInputStream();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			int i;
			do {
				i = is.read();
				if (i>=0) baos.write(i);
			} while (i>=0);
		} finally {
			is.close();
			baos.close();
		}
		baos.flush();
		String content = baos.toString();
		return content;
	}
	
	private static double[] getLatLngFromResponse(String s) {
		try {
	    	JSONObject bundle = new JSONObject(s);
	    	JSONArray resultsArray = bundle.getJSONArray("results");
	    	if (resultsArray.length()==0) {
	    		return null;
	    	}
	    	JSONObject latlng = ((JSONObject)(resultsArray.get(0))).
	    			getJSONObject("geometry").
	    			getJSONObject("location");
	    	double[] result = new double[2];
	    	result[0] = latlng.getDouble("lat");
	    	result[1] = latlng.getDouble("lng");
	    	return result;
		} catch (JSONException e) {
			throw new RuntimeException(s, e);
		}
	}
	
	public static String readTemplate(String filename, Map<String,String> fieldValues) {
		try {
			InputStream is = SynapseUserGeolocation.class.getClassLoader().getResourceAsStream(filename);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			StringBuilder sb = new StringBuilder();
			try {
				String s = br.readLine();
				while (s != null) {
					sb.append(s + "\r\n");
					s = br.readLine();
				}
				String template = sb.toString();
				if (fieldValues!=null) {
					for (String fieldMarker : fieldValues.keySet()) {
						template = template.replaceAll(fieldMarker, fieldValues.get(fieldMarker));
					}
				}
				return template;
			} finally {
				br.close();
				is.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	private static void uploadFile(String s3FileName, String content) throws IOException {
		String accessKey = getProperty("ACCESS_KEY");
		String secretKey = getProperty("SECRET_KEY");
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonS3Client client = new AmazonS3Client(awsCredentials);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("text/html");
		InputStream is = new ByteArrayInputStream(content.getBytes("utf-8"));
		try {
			client.putObject(BUCKET_NAME, s3FileName, is, metadata);
		} finally {
			is.close();
		}
	}

	private static Properties properties = null;

	public static void initProperties() {
		if (properties!=null) return;
		properties = new Properties();
		InputStream is = null;
    	try {
    		is = SynapseUserGeolocation.class.getClassLoader().getResourceAsStream("global.properties");
    		properties.load(is);
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	} finally {
    		if (is!=null) try {
    			is.close();
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	}
   }
	
	public static String getProperty(String key) {
		initProperties();
		String commandlineOption = System.getProperty(key);
		if (commandlineOption!=null) return commandlineOption;
		String embeddedProperty = properties.getProperty(key);
		if (embeddedProperty!=null) return embeddedProperty;
		// (could also check environment variables)
		throw new RuntimeException("Cannot find value for "+key);
	}	
	  
	private static SynapseClient createSynapseClient() {
			boolean staging = false;
			SynapseClientImpl scIntern = new SynapseClientImpl();
			if (staging) {
				scIntern.setAuthEndpoint("https://repo-staging.prod.sagebase.org/auth/v1");
				scIntern.setRepositoryEndpoint("https://repo-staging.prod.sagebase.org/repo/v1");
				scIntern.setFileEndpoint("https://repo-staging.prod.sagebase.org/file/v1");
			} else { // prod
				scIntern.setAuthEndpoint("https://repo-prod.prod.sagebase.org/auth/v1");
				scIntern.setRepositoryEndpoint("https://repo-prod.prod.sagebase.org/repo/v1");
				scIntern.setFileEndpoint("https://repo-prod.prod.sagebase.org/file/v1");
			}
			return SynapseProfileProxy.createProfileProxy(scIntern);

	  }
}
