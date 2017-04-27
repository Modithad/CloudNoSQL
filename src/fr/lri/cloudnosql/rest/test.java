package fr.lri.cloudnosql.rest;

import java.lang.reflect.Type;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import fr.lri.cloudnosql.ops.insert.InsertHandler;
import fr.lri.cloudnosql.ops.query.QueryBroker;
import fr.lri.cloudnosql.util.Util;

@Path("/ctofservice")
public class test {
	InsertHandler h = InsertHandler.getInstance();
	QueryBroker qb = new QueryBroker();

	@GET
	@Produces("application/xml")
	public String convertCtoF() {

		Double fahrenheit;
		Double celsius = 36.8;
		fahrenheit = ((celsius * 9) / 5) + 32;

		String result = "@Produces(\"application/xml\") Output: \n\nC to F Converter Output: \n\n" + fahrenheit;
		return "<ctofservice>" + "<celsius>" + celsius + "</celsius>" + "<ctofoutput>" + result + "</ctofoutput>"
				+ "</ctofservice>";
	}

	@Path("{c}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String convertCtoFfromInput(@PathParam("c") Double c) {
		Double fahrenheit;
		Double celsius = c;
		fahrenheit = ((celsius * 9) / 5) + 32;

		// sample s= new sample();
		// s.setName("output");
		// s.setValue(fahrenheit);
		Gson gson = new GsonBuilder().create();
		String json = gson.toJson(fahrenheit);
		// String result = "@Produces(\"application/xml\") Output: \n\nC to F
		// Converter Output: \n\n" + fahrenheit;
		return json;// "<ctofservice>" + "<celsius>" + celsius + "</celsius>" +
					// "<ctofoutput>" + result + "</ctofoutput>" +
					// "</ctofservice>";
	}

	@Path("/test3")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String test3(String m) {

		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		System.out.println("input is");
		System.out.println(m);
		String s = "{  \"noFollowers\" : 3284, \"noLists\" : 32, \"user_id\" : 274048336, \"name\" : \"SAN LQ_Singapore\", \"description\" : \"Created for my lovies. Be blessed to be a blessing to others\", \"location\" : \"Singapore\", \"noFriends\" :838, \"screenName\" : \"cordav0121\", \"noStatuses\" : 467608 }";
		System.out.println(s);
		Gson gson = new GsonBuilder().create();
		Map json = Util.toMap(new JSONObject(m));
		h.insertOracle(json);
		return m;
	}

	@Path("/test2")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String test2(String m) {

		// InsertHandler h = new InsertHandler();
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		System.out.println(m);
		Gson gson = new GsonBuilder().create();
		Map json = Util.toMap(new JSONObject(m));
		h.insert(json);
		return m;
	}

	@Path("/get1")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String get1(String m) {

		// InsertHandler h = new InsertHandler();
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		System.out.println("input is");
		System.out.println(m);
		Map json = Util.toMap(new JSONObject(m));
		Object o = h.getUser(json);
		Gson gson = new GsonBuilder().create();
		return gson.toJson(o);
	}

	@Path("/get2")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String get2(String m) {

		// InsertHandler h = new InsertHandler();
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		System.out.println("input is");
		System.out.println(m);
		Map json = Util.toMap(new JSONObject(m));
		Object o = h.getUser2(json);
		Gson gson = new GsonBuilder().create();
		return gson.toJson(o);
	}

	@Path("/getfriends")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String getfriends(String m) {

		// InsertHandler h = new InsertHandler();
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		System.out.println("input is");
		System.out.println(m);
		Map json = Util.toMap(new JSONObject(m));
		Object o = h.getFriends(json);
		Gson gson = new GsonBuilder().create();
		return gson.toJson(o);
	}
	
	
	@Path("/getTest")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public void getTest(String m) {

		System.out.println("input is");
		System.out.println(m);
//		Map json = Util.toMap(new JSONObject(m));
//		Object o = h.getFriends(json);
//		Gson gson = new GsonBuilder().create();
		qb.analyzeQuery(m);
	}
}
