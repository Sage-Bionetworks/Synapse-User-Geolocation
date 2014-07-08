package org.sagebionetworks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.SynapseProfileProxy;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserProfile;

/**
 * Hello world!
 *
 */
public class SynapseUserGeolocation 
{
	private static final int PAGE_SIZE = 50;
	
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
    }
    
    public static void geoLocate() throws Exception {
    	String synapseUserName = getProperty("SYNAPSE_USERNAME");
    	String synapsePassword = getProperty("SYNAPSE_PASSWORD");
        SynapseClient synapseClient = createSynapseClient();
        synapseClient.login(synapseUserName, synapsePassword);
    	long total = Integer.MAX_VALUE;
    	int upCount = 0;
    	int companyCount = 0;
    	int industryCount = 0;
    	int positionCount = 0;
    	int locationCount = 0;
    	int latLngCount = 0;
    	int geoLocatedUsersCount = 0;
    	int MAX_GEO_POSNS = 10000;
    	Map<String,JSONObject> geoLocMap = new HashMap<String,JSONObject>();
       	for (int offset=0; offset<total && latLngCount<MAX_GEO_POSNS; offset+=PAGE_SIZE) {
       		PaginatedResults<UserProfile> pr = synapseClient.getUsers(offset, PAGE_SIZE);
        	total = (int)pr.getTotalNumberOfResults();
        	List<UserProfile> page = pr.getResults();
        	for (int i=0; i<page.size() && latLngCount<MAX_GEO_POSNS; i++) {
        		UserProfile up = page.get(i);
        		upCount++;
        		if (!empty(up.getCompany())) companyCount++;
        		if (!empty(up.getIndustry())) industryCount++;
        		if (!empty(up.getPosition())) positionCount++;
        		if (!empty(up.getLocation())) locationCount++;
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
    	System.out.println(allInfo);
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
