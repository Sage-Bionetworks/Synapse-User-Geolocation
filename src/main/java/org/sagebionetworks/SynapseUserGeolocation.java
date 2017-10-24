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
import org.sagebionetworks.reflection.model.PaginatedResults;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.TeamMember;
import org.sagebionetworks.repo.model.UserProfile;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.IOUtils;

/**
 * Hello world!
 *
 */
public class SynapseUserGeolocation {
	private static final int PAGE_SIZE = 20;
	private static final String BUCKET_NAME = "geoloc.sagebase.org";
	private static final String JS_FILE_NAME = "geoLocate.js";
    private static final String MAIN_PAGE_FILE_TEMPLATE = "indexTemplate.html";
    private static final String MAIN_PAGE_FILE_NAME = "index.html";
    private static final String ALL_MARKERS_JSON = "allPoints.json";
    private static final String TEAM_PAGE_FILE_TEMPLATE = "teamPageTemplate.html";
    // this allows us to test without processing all users
    private static final int MAX_GEO_POSNS = 100000;
    private static final int MAX_CONSECUTIVE_FAILURES = 75;
    private static final String LATLNG_TAG = "latLng";
    private static final String LOCATION_TAG = "location";
    private static final String USER_IDS_TAG = "userIds";
    
    private static final String GOOGLE_RESULTS_FILE = "googleResults.json";
    
    private int googleRequestCount=0;
    private JSONObject googleResults = null;
	
    public static void main( String[] args ) throws Exception {
    	SynapseUserGeolocation sgl = new SynapseUserGeolocation();
    	sgl.geoLocate();
    }
    
    public void geoLocate() throws Exception {
    	String synapseUserName = getProperty("SYNAPSE_USERNAME");
    	String synapsePassword = getProperty("SYNAPSE_PASSWORD");
        SynapseClient synapseClient = SynapseClientFactory.createSynapseClient();
        synapseClient.login(synapseUserName, synapsePassword);
    	long total = 1L;
    	int latLngCount = 0;
    	int geoLocatedUsersCount = 0;
    	Map<String,JSONObject> geoLocMap = new HashMap<String,JSONObject>();
    	Map<String,String> userToLocationMap = new HashMap<String,String>();
    	
    	try {
    		String googleResultsString = downloadFile(GOOGLE_RESULTS_FILE);
    		googleResults = new JSONObject(googleResultsString);
    	} catch (Exception e) {
    		System.out.println("Exception trying to download file "+GOOGLE_RESULTS_FILE+". Will start from scratch.  Exception is: ");
    		googleResults = new JSONObject();
    	}
    	int numPreviouslyKnownLocations = googleResults.length();
    	googleRequestCount = 0;
    	int consecutiveFailures = 0;
       	for (int offset=0; offset<total && latLngCount<MAX_GEO_POSNS && consecutiveFailures<MAX_CONSECUTIVE_FAILURES; offset+=PAGE_SIZE) {
			System.out.println(""+offset+" of "+total);
       		PaginatedResults<UserProfile> pr = synapseClient.getUsers(offset, PAGE_SIZE);
        	total = (int)pr.getTotalNumberOfResults();
        	List<UserProfile> page = pr.getResults();
        	for (int i=0; i<page.size() && latLngCount<MAX_GEO_POSNS && consecutiveFailures<MAX_CONSECUTIVE_FAILURES; i++) {
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
        					replaceAll("Santo AndrÃÃÃÂ¯ÃÃÃÂ¿ÃÃÃÂ½", "Santo André").
           					replaceAll("Santo AndrÃÃÂ¯ÃÃÂ¿ÃÃÂ½", "Santo André").
        					replaceAll("QuÃ¯Â¿Â½bec, Canada", "Québec, Canada").
        					replaceAll("TÃÂ¯ÃÂ¿ÃÂ½bingen, Germany", "Tübingen, Germany").
        					replaceAll("TÃ¯Â¿Â½bingen, Germany", "Tübingen, Germany").
        					replaceAll("BogotÃ¯Â¿Â½, Colombia", "Bogota, Colombia").
        					replaceAll("Sï¿½o Paulo, Brazil", "Sao Paulo, Brazil");
        			JSONObject geoLocatedInfo = geoLocMap.get(fixedLocation);
        			if (geoLocatedInfo==null) {
        				double[] latlng = lookupLocation(fixedLocation);
        				if (latlng==null) {
        					if (fixedLocation.startsWith("Greater ") && fixedLocation.endsWith(" Area")) {
        						String upString = fixedLocation.substring(8, fixedLocation.length()-5).trim();
                 				latlng = lookupLocation(upString);
        					}
        					if (latlng==null) {
        						System.out.println("No result for "+fixedLocation);
        						consecutiveFailures++;
        					}
        				}
        				if (latlng!=null) {
        					consecutiveFailures=0;
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
        			} else { //geoLocatedInfo!=null, i.e. geoLocMap has a value for the given location
        				JSONArray userIds = (JSONArray)geoLocatedInfo.get(USER_IDS_TAG);
        				userIds.put(userIds.length(), up.getOwnerId());
    					userToLocationMap.put(up.getOwnerId(), fixedLocation);
    					geoLocatedUsersCount++;
        			}
        			latLngCount++;
        		}
        	}
       	}
       	if (consecutiveFailures>=MAX_CONSECUTIVE_FAILURES) {
       		System.out.println("Encountered "+consecutiveFailures+" consecutive failures.  May have reached the limit for API requests.");
       	}
       	
       	// now write the google results so we don't have to get them again
       	uploadFile(GOOGLE_RESULTS_FILE, googleResults.toString());

       	JSONArray allInfo = new JSONArray();
       	for (JSONObject gli : geoLocMap.values()) {
			allInfo.put(allInfo.length(), gli);
    	}
    	System.out.println("Number of previously geolocated names: "+numPreviouslyKnownLocations+
    			", number calls to Google geolocation service: "+googleRequestCount+
    			", number of geolocated names afterward: "+googleResults.length());
    	System.out.println("Total number of geolocated users: "+geoLocatedUsersCount+
    			".  Number of distinct locations: "+geoLocMap.size());
    	
    	// upload the js file
    	String jsContent = readTemplate(JS_FILE_NAME, null);
    	uploadFile(JS_FILE_NAME, jsContent);
    	
       	// add Team pages
    	Map<String,Team> nameToTeamMap = new TreeMap<String,Team>();
    	{
    		long totalNumberOfResults = 1L;
    		for (long offset=0; offset<totalNumberOfResults; offset+=PAGE_SIZE) {
    			PaginatedResults<Team> teamPRs = synapseClient.getTeams(null, PAGE_SIZE, offset);
    			totalNumberOfResults=teamPRs.getTotalNumberOfResults();
    			// sort teams by name
    			for (Team team : teamPRs.getResults()) nameToTeamMap.put(team.getName(), team);
    			
    			// don't overwhelm the server with requests
    			try {
    				Thread.sleep(5000L);
    			} catch (InterruptedException e) {
    				// continue
    			}
    		}
    	}
    	List<String> hyperLinks = new ArrayList<String>();
    	for (String name : nameToTeamMap.keySet()) {
    		Team team = nameToTeamMap.get(name);
    		long totalNumberOfResults =1L;
    		Map<String,JSONObject> teamGeoLocMap = new HashMap<String,JSONObject>();
    		for (long offset=0; offset<totalNumberOfResults; offset+=PAGE_SIZE) {
    			PaginatedResults<TeamMember> memberPRs = synapseClient.getTeamMembers(team.getId(), null, PAGE_SIZE, offset);
    			totalNumberOfResults = memberPRs.getTotalNumberOfResults();

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
        	uploadFile(team.getId()+".json", teamInfo.toString());
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
    	uploadFile(ALL_MARKERS_JSON, allInfo.toString());
    	
    	System.out.println("Finished uploading files to S3.  Visit http://s3.amazonaws.com/"+BUCKET_NAME+"/"+MAIN_PAGE_FILE_NAME);
    }
    
    private static final double EPSILON = 1e-2;
    
    private static JSONObject checkForDuplicate(String location, double[]latlng, Collection<JSONObject> values) throws JSONException {
    	for (JSONObject o : values) {
    		JSONArray a = (JSONArray)o.get(LATLNG_TAG);
    		if (Math.abs(latlng[0]-a.getDouble(0))<EPSILON && Math.abs(latlng[1]-a.getDouble(1))<EPSILON) {
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
    
    private static final String EMPTY_JSON = "{}";
    
    
    private double[] lookupLocation(String location) throws IOException, JSONException, InterruptedException {
    	// if the result is cached then don't look it up using the Google geolocation service
    	JSONArray jsonLatLng = googleResults.has(location) ? googleResults.getJSONArray(location) : null;
    	if (jsonLatLng!=null) {
    		double[] latlng = new double[2];
    		for (int i=0; i<2; i++) latlng[i]=jsonLatLng.getDouble(i);
    		return latlng;
    	}
		String encodedLocation = URLEncoder.encode(location, "utf-8");
		String urlString = "https://maps.googleapis.com/maps/api/geocode/json?sensor=true_or_false&address="+
				encodedLocation;
		String json = executeJsonQueryWithRetry(urlString);
		googleRequestCount++;
		Thread.sleep(500L);
		double[] latlng = getLatLngFromResponse(json);
		if (latlng!=null) {
			jsonLatLng = new JSONArray();
			for (int i=0; i<2; i++) jsonLatLng.put(i, latlng[i]);
			googleResults.put(location, jsonLatLng);
		}
		return latlng;
    }
    
	private static String executeJsonQueryWithRetry(String urlString) throws IOException {
		for (int i=0; i<RETRIES; i++) {
			try {
				return executeJsonQuery(urlString);
			} catch (IOException e) {
				if (i==RETRIES-1) return EMPTY_JSON;
				System.out.println("encountered exception for "+urlString);
			}
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				// continue retrying
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
			System.out.println("In getLatLngFromResponse, encountered exception and will return null: "+e.getMessage()); 
			return null;
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

	private static AmazonS3Client getS3Client() {
		String accessKey = getProperty("ACCESS_KEY");
		String secretKey = getProperty("SECRET_KEY");
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		return new AmazonS3Client(awsCredentials);
	}
	
	private static final String CHAR_SET="utf-8";
	
	private static void uploadFile(String s3FileName, String content) throws IOException {
    	System.out.println("Uploading "+s3FileName);
    	AmazonS3Client client = getS3Client();
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("text/html");
		InputStream is = new ByteArrayInputStream(content.getBytes(CHAR_SET));
		try {
			client.putObject(BUCKET_NAME, s3FileName, is, metadata);
		} finally {
			is.close();
		}
	}

	private static String downloadFile(String s3FileName) throws IOException {
    	AmazonS3Client client = getS3Client();
    	ByteArrayOutputStream baos=new ByteArrayOutputStream();
		String urlString = client.getResourceUrl(BUCKET_NAME, s3FileName);
		if (urlString.startsWith("https")) urlString = "http"+urlString.substring(5); // hack to use http, not https
    	System.out.println("Downloading: "+urlString);
		URL url = new URL(urlString);
		InputStream in = url.openStream();
		try {
			IOUtils.copy(in, baos);
		} finally {
			in.close();
			baos.close();
		}
		return baos.toString(CHAR_SET);
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
	  

}
