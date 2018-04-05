package fr.lri.cloudnosql.ops.query;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONObject;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

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

		String query1 = "INSERT INTO `master_thesis`.`system_user_search_hashtag_internal`(`iteration`,`time`)VALUES(?,?)";
		PreparedStatement stmt = null;
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/master_thesis?" + "user=root&password=root");
			stmt = conn.prepareStatement(query1);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Long startTime = System.currentTimeMillis();
		Map<String, Object> m = Util.toMap(new JSONObject(query));

		if (m.containsKey("select")) {
			Object o = analyzeSelect(m);
			Long endTime = System.currentTimeMillis();
			try {
				stmt.setInt(1, 1);
				stmt.setInt(2, (int) (endTime - startTime));
				stmt.execute();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return o;
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
