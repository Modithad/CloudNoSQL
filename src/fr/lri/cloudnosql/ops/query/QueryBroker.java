package fr.lri.cloudnosql.ops.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONObject;

import fr.lri.cloudnosql.db.meta.DBMeta;
import fr.lri.cloudnosql.distrib.DistributionHandler;
import fr.lri.cloudnosql.util.Util;

public class QueryBroker {

	private static Set<String> distKeys;
	private static Set<String> primKeys;
	private static Map<String, DBMeta> entities;
	private static DistributionHandler distHandler = DistributionHandler.getInstance();

	public QueryBroker() {
		distKeys = new HashSet<>();
		primKeys = new HashSet<>();
		distKeys.add("user_id");
		primKeys.add("user_id");
		primKeys.add("tweet_id");
	}

	public Object analyzeQuery(String query) {
		Map<String, Object> m = Util.toMap(new JSONObject(query));

		if (m.containsKey("select")) {
			return analyzeSelect(m);
		}
		return null;
	}

	public void analyzeInsert(Map<String, Object> map) {

	}

	public Object analyzeSelect(Map<String, Object> map) {
		ArrayList<String> entities = (ArrayList<String>) map.get("from");
		HashMap<String, Object> conditions = (HashMap<String, Object>) map.get("where");

		HashMap<String, HashMap<String, Object>> entityConditions = new HashMap<>();

		for (Entry<String, Object> ent : conditions.entrySet()) {
			String attribName = ent.getKey();
			String entityName = DistributionHandler.getEntityofValue(attribName);
			HashMap<String, Object> innerMap;
			if (entityConditions.containsKey(entityName)) {
				innerMap = entityConditions.get(ent);
			} else {
				innerMap = new HashMap<>();
			}

			innerMap.put(ent.getKey(), ent.getValue());
			entityConditions.put(entityName, innerMap);
		}

		for (String string : entities) {
			if (!entityConditions.containsKey(string)) {
				entityConditions.put(string, null);
			}
		}

		return distHandler.complexRequest2(entityConditions);
	}

	public void createMongoCondition(HashMap<String, Object> map) {

	}
}
