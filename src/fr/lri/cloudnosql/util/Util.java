package fr.lri.cloudnosql.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Util {

	public static Map<String, Object> toMap(JSONObject object) throws JSONException {
		Map<String, Object> map = new HashMap<String, Object>();

		Iterator<String> keysItr = object.keys();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			map.put(key, value);
		}
		return map;
	}

	public static List<Object> toList(JSONArray array) throws JSONException {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); i++) {
			Object value = array.get(i);
			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			list.add(value);
		}
		return list;
	}

	public static Map<String, Object> joinMap(Map<String, Object> parent, Object child, String relation) {

		parent.put(relation, child);

		return parent;

	}

	public Map<Object, List<Document>> filter(List<Map> map, String field, Function<Long, String> func) {
		System.out.println(map.get(0).getClass());

		Map<Object, List<Document>> index = map.stream().map(x -> new Document(x))
				.collect(Collectors.groupingBy(l -> func.apply((long) l.get(field))));

		System.out.println(index);
		return index;
	}

	public static Map<Object, List<Long>> SplitToServers(List<?> map, Function<Long, String> func) {
		Map<Object, List<Long>> index = map.stream().map(x -> getLong(x))
				.collect(Collectors.groupingBy(l -> func.apply(l)));

		return index;
	}

	public static long getLong(Object obj) {
		long id;
		if (obj.getClass().equals(Integer.class)) {
			id = Long.valueOf((int) obj);
		} else
			id = (long) obj;
		return id;
	}

	private static List<Object> joinLists(Set<List<Object>> lists) {

		List<Object> output = new ArrayList<>();

		for (List<Object> list : lists) {
			output.addAll(list);
		}

		return output;

	}

	public static String findServer(long l, Map<Long, String> servers) {
		System.out.println(l);
		Long[] arr = servers.keySet().toArray(new Long[0]);
		for (int i = 0; i < arr.length; i++) {
			if (l >= arr[i] && i == arr.length - 1) {
				System.out.println("one");
				return (servers.get(arr[i]));
			} else if (l >= arr[i] && l < arr[i + 1]) {
				System.out.println("two");
				return (servers.get(arr[i]));
			}
		}
		return null;
	}
}
