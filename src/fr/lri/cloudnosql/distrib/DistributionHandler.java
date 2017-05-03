package fr.lri.cloudnosql.distrib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import fr.lri.cloudnosql.db.OracleNoSQL.OracleNoManager;
import fr.lri.cloudnosql.db.meta.DBMeta;
import fr.lri.cloudnosql.db.meta.DBType;
import fr.lri.cloudnosql.db.meta.IDBManager;
import fr.lri.cloudnosql.db.mongo.MongoManager;
import fr.lri.cloudnosql.util.Util;

/*
 * A singleton used to keep the data distrubution, entity and key information
 * 
 */
/**
 * @author Moditha
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

	private static String distribKey;

	private static ArrayList<String> primKeys = new ArrayList<>();

	private static String rootEntity;

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
		userMeta.setLevel(0);
		keyEntityLookup.put("user_id", "user");
		distribKey = "user_id";
		rootEntity = "user";

		DBMeta tweetMeta = new DBMeta();
		tweetMeta.setDBType(DBType.ORACLE_NOSQL);
		tweetMeta.setEntity("tweet");
		tweetMeta.setDBName("kvstore");
		tweetMeta.addPrimKey("tweet_id");
		tweetMeta.addReverseKey("user_id");
		keyEntityLookup.put("tweet_id", "tweet");
		primKeys.add("tweet_id");
		tweetMeta.setLevel(1);

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
					} else if (ent.getValue().getDBType() == DBType.ORACLE_NOSQL) {
						OracleNoManager mgr1 = new OracleNoManager("kvstore", distrib.getValue(), "5000");
						managerMap.put(distrib.getValue(), mgr1);
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

			if (elm.getValue() != null) {
				if (!elm.getValue().keySet().contains(distribKey)) {
					// get the distrib key to find the server
					Long distKeyValue = getId(elm.getValue(), distribKey);

					DBType type = entityMeta.get(elm.getKey()).getDBType();
					if (type == DBType.MONGO_DB) {
						// entityDBMap.get(elm.getKey());
						System.out.println(elm.getKey());
						System.out.println(distribMapper);
						MongoManager m = (MongoManager) entityDBMap.get(elm.getKey())
								.get(Util.findServer(distKeyValue, distribMapper.get(distribKey)));
						System.out.println(m.get(elm.getValue()));
					}

				}
			}
		}
	}

	public void complexRequest(Map<String, HashMap<String, Object>> entityQuery) {

		System.out.println(entityQuery);

		Queue<DBQueueObject> queryQueue = new PriorityQueue<>();
		String server = null;
		Long distKeyValue = null;
		for (Entry<String, HashMap<String, Object>> elm : entityQuery.entrySet()) {

			// if the query has a distribution key involved in the where clause
			DBMeta meta = entityMeta.get(elm.getKey());
			DBType type = meta.getDBType();

			if (elm.getValue() != null) {
				if (elm.getValue().keySet().contains(distribKey)) {
					// get the distrib key to find the server
					distKeyValue = getId(elm.getValue(), distribKey);
					if (entityQuery.entrySet().size() == 1) {

						if (type == DBType.MONGO_DB) {
							// entityDBMap.get(elm.getKey());
							System.out.println(elm.getKey());
							System.out.println(distribMapper);
							server = Util.findServer(distKeyValue, distribMapper.get(distribKey));
							MongoManager m = (MongoManager) entityDBMap.get(elm.getKey())
									.get(Util.findServer(distKeyValue, distribMapper.get(distribKey)));
							System.out.println(m.get(elm.getValue()));
						}

					} else {
						DBQueueObject obj = new DBQueueObject();
						obj.setQuery(elm.getValue());
						if (type == DBType.MONGO_DB) {
							obj.setManager((MongoManager) entityDBMap.get(elm.getKey())
									.get(Util.findServer(distKeyValue, distribMapper.get(distribKey))));
						} else if (type == DBType.ORACLE_NOSQL) {
							obj.setManager((OracleNoManager) entityDBMap.get(elm.getKey())
									.get(Util.findServer(distKeyValue, distribMapper.get(distribKey))));
						}
						queryQueue.add(obj);
					}
				} else {
					// only tweetid
					// send requests to all servers
					// get the one with result
					// resend the request with major minor

					Object m = getFromPrimKeyThroughRoot(elm.getValue(), meta);
				}
			} else {

			}

		}
	}

	public Object complexRequest2(Map<String, HashMap<String, Object>> entityQuery) {

		Object ret = null;
		String server = null;
		// querying with one entity and retrieving one no joins
		if (entityQuery.size() == 1) {
			Entry<String, HashMap<String, Object>> entity = entityQuery.entrySet().iterator().next();
			DBMeta meta = entityMeta.get(entity.getKey());

			// query contains only one filter
			if (entity.getValue().size() == 1) {
				// filter is a distribution key
				if (entity.getValue().keySet().contains(distribKey)) {
					Long distKeyValue = getId(entity.getValue(), distribKey);
					if (meta.getDBType() == DBType.MONGO_DB) {
						// entityDBMap.get(elm.getKey());
						server = Util.findServer(distKeyValue, distribMapper.get(distribKey));
						MongoManager m = (MongoManager) entityDBMap.get(entity.getKey()).get(server);
						ret = m.get(entity.getValue());
					}
				}
				// filter is a primary key assuming directly connected
				else if (!Collections.disjoint(entity.getValue().keySet(), primKeys)
						&& meta.getReverseKeys().contains(distribKey)) {
					ret = getFromPrimKeyThroughRoot(entity.getValue(), meta);
				}
			}
		} else if (entityQuery.size() == 2) {
			// querying with distribution key and another entity
			if (entityQuery.keySet().contains(rootEntity) && entityQuery.get(rootEntity).containsKey(distribKey)) {
				// get the root entity
				Long distKeyValue = getId(entityQuery.get(rootEntity), distribKey);
				DBMeta meta = entityMeta.get(rootEntity);
				if (meta.getDBType() == DBType.MONGO_DB) {
					server = Util.findServer(distKeyValue, distribMapper.get(distribKey));
					MongoManager m = (MongoManager) entityDBMap.get(rootEntity).get(server);
					ret = m.get(entityQuery.get(rootEntity));
				}
				entityQuery.remove(rootEntity);
				Entry<String, HashMap<String, Object>> child = entityQuery.entrySet().iterator().next();
				meta = entityMeta.get(child.getKey());
				if (meta.getReverseKeys().contains(distribKey)) {
					if (meta.getDBType() == DBType.ORACLE_NOSQL) {
						OracleNoManager mgr = (OracleNoManager) entityDBMap.get(child.getKey()).get(server);
						Object obj = mgr.getMulti(String.valueOf(distKeyValue));
						ret = Util.joinMap((Map<String, Object>) ret, obj, child.getKey() + "s");
					}
				}
			}
		}
		return ret;
	}

	// private Map<String, HashMap<String, Object>> getNonRoot(Map<String,
	// HashMap<String, Object>> withRoot) {
	// Map<String, HashMap<String, Object>> map = new HashMap<>();
	// for (Entry<String, HashMap<String, Object>> item : withRoot.entrySet()) {
	// if(){
	//
	// }
	// }
	// return map;
	// }

	/**
	 * finds the containing server given a primary key attributed query and the
	 * metadata and returns the queried object
	 * 
	 * @param query
	 *            query represented in a map with a primary key that connects
	 *            drectly to the root
	 * @param meta
	 *            meta info of the queried entity
	 * @return queried object with the primary key
	 */
	private Object getFromPrimKeyThroughRoot(HashMap<String, Object> query, DBMeta meta) {

		// get all the managers for the entity

		Map<String, IDBManager> managers = entityDBMap.get(meta.getEntity());
		if (meta.getDBType() == DBType.ORACLE_NOSQL) {
			for (IDBManager manager : managers.values()) {
				// get the reverse index key value to get the root and the
				// server
				Object o = ((OracleNoManager) manager).get(String.valueOf(query.get(meta.getPrimKeys().get(0))));
				if (o != null) {
					System.out.println("not null in " + ((OracleNoManager) manager).hname);
					// when the server is found execute the key combination to
					// retrieve the data
					return reverseLookup(manager, String.valueOf(o),
							String.valueOf(query.get(meta.getPrimKeys().get(0))));
				}
			}
		}

		return query;

	}

	/**
	 * do the reverse lookup and retrieves the data given the manager and major
	 * and minor keys for one to many relationships
	 * 
	 * @param manager
	 *            IDBmanager
	 * @param majorKey
	 *            key of the root element (one side)
	 * @param minorkey
	 *            key of the related element (many side)
	 * @return retrieved object
	 */
	private Object reverseLookup(IDBManager manager, String majorKey, String minorkey) {
		if (manager.getDBType() == DBType.ORACLE_NOSQL) {
			Object o = ((OracleNoManager) manager).get(majorKey, minorkey);
			return o;
		}
		return null;
	}

	/**
	 * returns the distributed key id if the query contains the distribution key
	 * 
	 * @param map
	 *            query represented as a map
	 * @param distKey
	 *            the distribution key
	 * @return distribution key value in the query
	 */
	private long getId(Map<String, Object> map, String distKey) {
		long id;
		if (map.get(distKey).getClass().equals(Integer.class)) {
			id = Long.valueOf((int) map.get(distKey));
		} else
			id = (long) map.get(distKey);
		return id;
	}
}
