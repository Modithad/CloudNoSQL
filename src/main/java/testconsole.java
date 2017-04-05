package main.java;


import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.mongodb.util.JSON;

import fr.lri.cloudnosql.ops.insert.InsertHandler;
import fr.lri.cloudnosql.util.Util;

public class testconsole {



	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Neo4jManager m = new Neo4jManager();
//		MongoManager mon = new MongoManager("localhost", "mydb");
//		mon.put(m.getUsers());
		
		
		//InsertHandler h= new InsertHandler();
		
	//	List<Map> users = m.getUsers();
//		for (Map<String, Object> map : users) {
//			h.insert(map);
//		}
		
		//h.filter(users, "user_id");
		//h.insert(users);
		Type type =  new TypeToken<Map<String, Object>>() {	}.getType();
		String s="{  \"noFollowers\" : 3284, \"noLists\" : 32, \"user_id\" : 274048336, \"name\" : \"SAN LQ_Singapore\", \"description\" : \"Created for my lovies. Be blessed to be a blessing to others\", \"location\" : \"Singapore\", \"noFriends\" :838, \"screenName\" : \"cordav0121\", \"noStatuses\" : 467608 }";
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(Double.class,  new JsonSerializer<Double>() {

		    public JsonElement serialize(Double src, Type typeOfSrc,
		                JsonSerializationContext context) {
		            Integer value = (int)Math.round(src);
		            return new JsonPrimitive(value);
		        }
		    });

		Gson gs = gsonBuilder.create();
		
		Map json = Util.toMap(new JSONObject(s));
		System.out.println(json.get("user_id").getClass());
	}
	

public static Map<String, Object> toMap(JSONObject object) throws JSONException {
    Map<String, Object> map = new HashMap<String, Object>();

    Iterator<String> keysItr = object.keys();
    while(keysItr.hasNext()) {
        String key = keysItr.next();
        Object value = object.get(key);

        if(value instanceof JSONArray) {
            value = toList((JSONArray) value);
        }

        else if(value instanceof JSONObject) {
            value = toMap((JSONObject) value);
        }
        map.put(key, value);
    }
    return map;
}



public static List<Object> toList(JSONArray array) throws JSONException {
		List<Object> list = new ArrayList<Object>();
    for(int i = 0; i < array.length(); i++) {
        Object value = array.get(i);
        if(value instanceof JSONArray) {
            value = toList((JSONArray) value);
        }

        else if(value instanceof JSONObject) {
            value = toMap((JSONObject) value);
        }
        list.add(value);
    }
    return list;
}

}
