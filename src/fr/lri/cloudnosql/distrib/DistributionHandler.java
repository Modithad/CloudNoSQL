package fr.lri.cloudnosql.distrib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import fr.lri.cloudnosql.db.meta.DBMeta;
import fr.lri.cloudnosql.db.meta.DBType;
import fr.lri.cloudnosql.db.meta.IDBManager;
import fr.lri.cloudnosql.db.mongo.MongoManager;
import fr.lri.cloudnosql.util.Util;

/*
 * A singleton used to keep the data distrubution, entity and key information
 * 
 */
public class DistributionHandler {

	private static DistributionHandler instance = null;

	public static DistributionHandler getInstance() {
		if (instance == null) {
			instance = new DistributionHandler();
		}
		return instance;
	}

	private ZooKeeperConnection zooCon;
	// DB connections for each of the entities
	private Map<String, Map<String, IDBManager>> entityDBMap = new HashMap<>();
	// Metadata of each of the entities including the DB details
	private Map<String, DBMeta> entityMeta = new HashMap<>();;
	// distribution keys and the mapping
	private Map<String, Map<Long, String>> distribMapper = new HashMap<>();
	//
	private static Map<String, String> keyEntityLookup = new HashMap<>();

	private static ArrayList<String> distribKeys = new ArrayList<>();

	private static ArrayList<String> primKeys = new ArrayList<>();

	// public DistributionHandler(String zkHost) throws IOException,
	// InterruptedException {
	// zooCon = new ZooKeeperConnection();
	// zooCon.connect(zkHost);
	// }

	public DistributionHandler() {
		createEntityMeta();
		createDistribMapper();
		createEntityDBMap();
	}

	private void createEntityMeta() {
		DBMeta userMeta = new DBMeta();
		userMeta.setDBType(DBType.MONGO_DB);
		userMeta.setEntity("user");
		userMeta.setDBName("user");
		userMeta.setCollection("testusers");
		userMeta.addPrimKey("user_id");
		userMeta.addDistKey("user_id");
		keyEntityLookup.put("user_id", "user");
		distribKeys.add("user_id");

		DBMeta tweetMeta = new DBMeta();
		tweetMeta.setDBType(DBType.ORACLE_NOSQL);
		tweetMeta.setEntity("tweet");
		tweetMeta.setDBName("kvstore");
		tweetMeta.addPrimKey("tweet_id");
		tweetMeta.addReverseKey("user_id");
		keyEntityLookup.put("tweet_id", "tweet");
		primKeys.add("tweet_id");

		entityMeta.put("user", userMeta);
		entityMeta.put("tweet", tweetMeta);
	}

	private void createDistribMapper() {
		Map<Long, String> userIdMap = new HashMap<>();
		userIdMap.put(0L, "tipi80");
		userIdMap.put(123593233L, "tipi81");
		userIdMap.put(444928949L, "tipi89");
		userIdMap.put(2364715460L, "tipi90");
		distribMapper.put("user_id", userIdMap);
	}

	private void createEntityDBMap() {
		for (Entry<String, DBMeta> ent : entityMeta.entrySet()) {
			for (Entry<String, Map<Long, String>> distribSet : distribMapper.entrySet()) {
				Map<String, IDBManager> managerMap = new HashMap<>();
				for (Entry<Long, String> distrib : distribSet.getValue().entrySet()) {
					if (ent.getValue().getDBType() == DBType.MONGO_DB) {
						MongoManager mgr = new MongoManager(distrib.getValue(), ent.getValue().getDBName());
						managerMap.put(distrib.getValue(), mgr);
					}
				}
				entityDBMap.put(ent.getKey(), managerMap);
			}
		}
	}

	public static String getEntityofValue(String val) {
		return keyEntityLookup.get(val);
	}

	public void simpleRequest(Map<String, HashMap<String, Object>> entityQuery) {

		System.out.println(entityQuery);

		for (Entry<String, HashMap<String, Object>> elm : entityQuery.entrySet()) {

			// if the query has a distribution key involved in the where clause

			System.out.println(elm.getValue());
			System.out.println(distribKeys);
			if (elm.getValue() != null) {
				if (!Collections.disjoint(elm.getValue().keySet(), distribKeys)) {
					// get the distrib key to find the server
					Long distKeyValue = getId(elm.getValue(), distribKeys.get(0));

					DBType type = entityMeta.get(elm.getKey()).getDBType();
					if (type == DBType.MONGO_DB) {
						// entityDBMap.get(elm.getKey());
						System.out.println(elm.getKey());
						System.out.println(distribMapper);
						MongoManager m = (MongoManager) entityDBMap.get(elm.getKey())
								.get(Util.findServer(distKeyValue, distribMapper.get(distribKeys.get(0))));
						System.out.println(m.get(elm.getValue()));
					}

				}
			}
		}
	}

	private long getId(Map<String, Object> map, String distKey) {
		long id;
		if (map.get(distKey).getClass().equals(Integer.class)) {
			id = Long.valueOf((int) map.get(distKey));
		} else
			id = (long) map.get(distKey);
		return id;
	}
}
