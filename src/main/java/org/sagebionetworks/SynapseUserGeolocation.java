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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.SynapseProfileProxy;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.TeamMember;
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
	private static final int PAGE_SIZE = 500;
	private static final String BUCKET_NAME = "geoloc.sagebase.org";
	private static final String JS_FILE_NAME = "geoLocate.js";
    private static final String MAIN_PAGE_FILE_TEMPLATE = "indexTemplate.html";
    private static final String MAIN_PAGE_FILE_NAME = "index.html";
    private static final String TEAM_PAGE_FILE_TEMPLATE = "teamPageTemplate.html";
    // this allows us to test without processing all users
    private static final int MAX_GEO_POSNS = 100000;
    private static final String LATLNG_TAG = "latLng";
    private static final String LOCATION_TAG = "location";
    private static final String USER_IDS_TAG = "userIds";
   
	
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
    	Map<String,String> userToLocationMap = new HashMap<String,String>();
       	for (int offset=0; offset<total && latLngCount<MAX_GEO_POSNS; offset+=PAGE_SIZE) {
			System.out.println(""+offset+" of "+total);
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
        					replaceAll("Zï¿½rich", "Zürich").
        					replaceAll("DÃ¯Â¿Â½sseldorf, Germany", "Düsseldorf, Germany").
        					replaceAll("Santo AndrÃÃÃÂ¯ÃÃÃÂ¿ÃÃÃÂ½, Brazil", "Santo André, Brazil").
        					replaceAll("QuÃ¯Â¿Â½bec, Canada", "Québec, Canada").
        					replaceAll("TÃÂ¯ÃÂ¿ÃÂ½bingen, Germany", "Tübingen, Germany").
        					replaceAll("BogotÃ¯Â¿Â½, Colombia", "Bogota, Colombia").
        					replaceAll("Sï¿½o Paulo, Brazil", "Sao Paulo, Brazil");
        			JSONObject geoLocatedInfo = geoLocMap.get(fixedLocation);

        			if (geoLocatedInfo==null) {
        				String encodedLocation = URLEncoder.encode(fixedLocation, "utf-8");
        				String urlString = "https://maps.googleapis.com/maps/api/geocode/json?sensor=true_or_false&address="+
        						encodedLocation;
        				String json = executeJsonQueryWithRetry(urlString);
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
                				json = executeJsonQueryWithRetry(urlString);
                				Thread.sleep(500L);
                				latlng = getLatLngFromResponse(json);
        					}
        					if (latlng==null) {
        						System.out.println("No result for "+fixedLocation);
        					}
        				}
        				if (latlng!=null) {
        					// if there is an existing lat/lng that matches this one, then merge
        					geoLocatedInfo = checkForDuplicate(fixedLocation, latlng, geoLocMap.values());
        					if (geoLocatedInfo == null) {
        						geoLocatedInfo = initializeGeoLocInfo(fixedLocation, up.getOwnerId());
        						JSONArray jsonLatLng = (JSONArray)geoLocatedInfo.get(LATLNG_TAG);
        						for (int ll=0; ll<2; ll++) jsonLatLng.put(ll, latlng[ll]);
        						geoLocMap.put(fixedLocation, geoLocatedInfo);
        					} else {
        						fixedLocation = geoLocatedInfo.getString(LOCATION_TAG);
        					}
        					userToLocationMap.put(up.getOwnerId(), fixedLocation);
        					geoLocatedUsersCount++;
        				}
        			} else {
        				JSONArray userIds = (JSONArray)geoLocatedInfo.get(USER_IDS_TAG);
        				userIds.put(userIds.length(), up.getOwnerId());
    					userToLocationMap.put(up.getOwnerId(), fixedLocation);
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
    	
    	// upload the js file
    	String jsContent = readTemplate(JS_FILE_NAME, null);
    	uploadFile(JS_FILE_NAME, jsContent);
    	
       	// add Team pages
    	PaginatedResults<Team> teamPRs = synapseClient.getTeams(null, Integer.MAX_VALUE, 0);
    	Map<String,Team> nameToTeamMap = new TreeMap<String,Team>();
    	// sort teams by name
    	for (Team team : teamPRs.getResults()) nameToTeamMap.put(team.getName(), team);
    	List<String> hyperLinks = new ArrayList<String>();
    	for (String name : nameToTeamMap.keySet()) {
    		Team team = nameToTeamMap.get(name);
        	PaginatedResults<TeamMember> memberPRs = synapseClient.getTeamMembers(team.getId(), null, Integer.MAX_VALUE, 0);
        	Map<String,JSONObject> teamGeoLocMap = new HashMap<String,JSONObject>();
        	for (TeamMember member : memberPRs.getResults()) {
        		String userId = member.getMember().getOwnerId();
        		String location = userToLocationMap.get(userId);
        		if (location==null) continue;
        		JSONObject geoLocatedInfo = geoLocMap.get(location);
        		if (geoLocatedInfo==null) throw 
        			new IllegalStateException(userId+" maps to "+location+" but this location has no value in 'geoLocMap'");
        		JSONObject teamGeoLocatedInfo = teamGeoLocMap.get(location);
        		if (teamGeoLocatedInfo==null) {
        			teamGeoLocatedInfo = initializeGeoLocInfo(location, userId);
					JSONArray srcLatLng = (JSONArray)geoLocatedInfo.get(LATLNG_TAG);
					JSONArray dstLatLng = (JSONArray)teamGeoLocatedInfo.get(LATLNG_TAG);
					for (int ll=0; ll<2; ll++) dstLatLng.put(ll, srcLatLng.getDouble(ll));
					teamGeoLocMap.put(location, teamGeoLocatedInfo);
        		} else {
        			JSONArray userIds = (JSONArray)teamGeoLocatedInfo.get(USER_IDS_TAG);
        			userIds.put(userIds.length(), userId);
        		}
        	}
        	// don't create a page for Teams having no geolocated members
        	if (teamGeoLocMap.isEmpty()) continue;
        	JSONArray teamInfo = new JSONArray();
        	for (JSONObject teamGeoLocatedInfo : teamGeoLocMap.values()) teamInfo.put(teamInfo.length(), teamGeoLocatedInfo);
        	Map<String,String> fieldValues = new HashMap<String,String>();
        	fieldValues.put("##teamName##", name);
        	fieldValues.put("##teamId##", team.getId());
        	fieldValues.put("##geoLocInfo##", teamInfo.toString());
        	String teamPageContent = readTemplate(TEAM_PAGE_FILE_TEMPLATE, fieldValues);
        	String fileName = team.getId()+".html";
        	uploadFile(fileName, teamPageContent);
        	hyperLinks.add("<a href="+fileName+">"+name+"</a><br/>");
    	}
    	
    	// upload the main page
    	Map<String,String> fieldValues = new HashMap<String,String>();
    	fieldValues.put("##geoLocInfo##", allInfo.toString());
    	fieldValues.put("##numberOfUsers##", ""+geoLocatedUsersCount);
    	// add hyperlinks to Team pages
    	StringBuilder sb = new StringBuilder();
    	for (String hyperlink : hyperLinks) sb.append(hyperlink);
    	fieldValues.put("##teamPageLinks##", sb.toString());
    	String mainPageContent = readTemplate(MAIN_PAGE_FILE_TEMPLATE, fieldValues);
    	
    	uploadFile(MAIN_PAGE_FILE_NAME, mainPageContent);
    	
   	    	
    	
    	System.out.println("Finished uploading files to S3.  Visit http://s3.amazonaws.com/"+BUCKET_NAME+"/"+MAIN_PAGE_FILE_NAME);
    }
    
    private static final double EPSILON = 1e-4;
    
    private static JSONObject checkForDuplicate(String location, double[]latlng, Collection<JSONObject> values) throws JSONException {
    	for (JSONObject o : values) {
    		JSONArray a = (JSONArray)o.get(LATLNG_TAG);
    		if (Math.abs(latlng[0]-a.getDouble(0))<EPSILON && Math.abs(latlng[1]-a.getDouble(1))<EPSILON) {
    			//System.out.println(location+" is colocated with "+o.get(LOCATION_TAG));
    			return o;
    		}
    	}
    	return null;
    }
    
    private static JSONObject initializeGeoLocInfo(String location, String userId) throws JSONException {
    	JSONObject  geoLocatedInfo = new JSONObject();
		geoLocatedInfo.put(LOCATION_TAG, location);
		JSONArray jsonLatLng = new JSONArray();
		geoLocatedInfo.put(LATLNG_TAG, jsonLatLng);
		JSONArray userIds = new JSONArray();
		userIds.put(userIds.length(), userId);
		geoLocatedInfo.put(USER_IDS_TAG, userIds);
		return geoLocatedInfo;
    }
    
    private static boolean empty(String s) {
    	return s==null || s.length()==0;
    }
    
    private static final int RETRIES = 3;
    
	private static String executeJsonQueryWithRetry(String urlString) throws IOException {
		for (int i=0; i<RETRIES; i++) {
			try {
				return executeJsonQuery(urlString);
			} catch (IOException e) {
				if (i==RETRIES-1) throw e;
				System.out.println("encountered exception for "+urlString);
			}
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		throw new IllegalStateException(); // shouldn't reach this line
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
    	System.out.println("Uploading "+s3FileName);
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
