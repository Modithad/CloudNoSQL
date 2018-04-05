package fr.lri.cloudnosql.ops.insert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.bson.Document;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import fr.lri.cloudnosql.db.OracleNoSQL.OracleNoManager;
import fr.lri.cloudnosql.db.meta.IDBManager;
import fr.lri.cloudnosql.db.mongo.MongoManager;
import fr.lri.cloudnosql.util.Util;
import oracle.kv.Key;

public class InsertHandler {
	private Map<Long, String> mongoServers = new LinkedHashMap<>();
	private Map<String, MongoManager> mongoManagers = new LinkedHashMap<>();

	private Map<String, OracleNoManager> oracleManagers = new LinkedHashMap<>();

	private static InsertHandler instance = null;

	public static InsertHandler getInstance() {
		if (instance == null) {
			System.out.println("creating new");
			instance = new InsertHandler();
		}
		return instance;
	}

	protected InsertHandler() {
		mongoServers.put(0L, "tipi80");
		mongoServers.put(123593233L, "tipi81");
		mongoServers.put(444928949L, "tipi89");
		mongoServers.put(2364715460L, "tipi90");

		for (Long key : mongoServers.keySet()) {
			String server = mongoServers.get(key);
			MongoManager manager = new MongoManager(server, "user");
			mongoManagers.put(server, manager);
			OracleNoManager mgr = new OracleNoManager("kvstore", server, "5000");
			oracleManagers.put(server, mgr);
		}
	}

	public String findServer(long l) {
		System.out.println(l);
		Long[] arr = mongoServers.keySet().toArray(new Long[0]);
		for (int i = 0; i < arr.length; i++) {
			if (l >= arr[i] && i == arr.length - 1) {
				return (mongoServers.get(arr[i]));
			} else if (l >= arr[i] && l < arr[i + 1]) {
				return (mongoServers.get(arr[i]));
			}
		}
		return null;
	}

	// public Map<Object, List<Document>> filter(List<Map> map, String field) {
	// System.out.println(map.get(0).getClass());
	//
	// Map<Object, List<Document>> index = map.stream().map(x-> new Document(x))
	// .collect(Collectors.groupingBy(l -> findServer((long) l.get(field))));
	//
	// System.out.println(index);
	// return index;
	// }

	// public Map<Object, List<Map<String, Object>>> filter(List<Map> map,
	// String field) {
	// Map<Object, List<Map<String, Object>>> index = map.stream()
	// .collect(Collectors.groupingBy(l -> findServer((long) l.get(field))));
	// return index;
	// }
	public void insert(Map<String, Object> map) {
		long id = getId(map);
		String server = findServer(id);
		mongoManagers.get(server).put(map);
	}

	public void insertOracle(Map<String, Object> map) {
		long id = getId(map);

		String tweetId = String.valueOf(map.get("tweet_id"));
		System.out.println(tweetId);
		String server = findServer(id);
		oracleManagers.get(server).put(tweetId, id);

		map.remove("user_id");

		oracleManagers.get(server).put(String.valueOf(id), tweetId, map);
	}

	public void insertHashtag(Map<String, Object> map) {
		// long id = getId(map);

		String tweetId = String.valueOf(map.get("tweet_id"));

		System.out.println(tweetId);
		for (OracleNoManager manager : oracleManagers.values()) {
			// get the reverse index key value to get the root and the
			// server
			Object o = manager.get(tweetId);
			if (o != null) {

				// Long userId=(Long) o;

				if (map.get("hashtags") != null) {
					HashMap tweet = (HashMap) manager.get(String.valueOf(o), tweetId);
					// System.out.println(tweet);
					tweet.put("hashtags", map.get("hashtags"));
					// System.out.println(tweet);
					manager.put(String.valueOf(o), tweetId, tweet);
					ArrayList l = (ArrayList) map.get("hashtags");

					for (Object object : l) {
						System.out.println(object);
						Set tweets = new HashSet();
						Object obj = manager.get("hashtag", String.valueOf(object));
						if (obj != null) {
							tweets = (Set) obj;
						}

						tweets.add(tweetId);
						manager.put("hashtag", String.valueOf(object), tweets);
						// System.out.println("Inserted "+manager.get("hashtag",
						// String.valueOf(object)));
					}

				}
				// System.out.println("not null in " + ((OracleNoManager)
				// manager).hname);
				// when the server is found execute the key combination to
				// retrieve the data
				// Key k= Key.createKey(Arrays.asList("Larry", "Moe", "Curly"));
				// manager.put(key, val);
			}
		}

		// String server = findServer(id);
		// oracleManagers.get(server).put(tweetId, id);
		//
		// map.remove("user_id");
		//
		// oracleManagers.get(server).put(String.valueOf(id), tweetId, map);
	}

	public void getHashtagUser(Map<String, Object> map) {
		for (OracleNoManager manager : oracleManagers.values()) {
			Object o = manager.get("hashtag", "one");
			if (o != null) {
				System.out.println(o.getClass());
			}
		}
	}

	private long getId(Map<String, Object> map) {
		long id;
		if (map.get("user_id").getClass().equals(Integer.class)) {
			id = Long.valueOf((int) map.get("user_id"));
		} else
			id = (long) map.get("user_id");
		return id;
	}

	public Object getUser(Map m) {
		long id = getId(m);
		return mongoManagers.get(findServer(id)).get(m);

		// return func;

	}

	public Object getUser2(Map m) {
		long id = getId(m);
		// Util.joinMap((Map<String, Object>)
		// mongoManagers.get(findServer(id)).get(m),
		// oracleManagers.get(findServer(id)).getMulti(String.valueOf(id)),
		// "tweets");

		return Util.joinMap((Map<String, Object>) mongoManagers.get(findServer(id)).get(m),
				oracleManagers.get(findServer(id)).getMulti(String.valueOf(id)), "tweets");
		// oracleManagers.get(findServer(id)).getMulti(String.valueOf(id));

		// return func;

	}

	public Object getFriends(Map m) {
		long id = getId(m);
		Document s = (Document) getUser(m);
		List<Object> l = (List<Object>) s.get("followers");
		Map<Object, List<Long>> map = Util.SplitToServers(l, func);
		List<Object> list = new ArrayList<>();

		for (Object object : map.keySet()) {
			list.addAll(mongoManagers.get(object).getIn("user_id", map.get(object)));
		}

		return list;
	}
	// public void insert(List<Map> map) {
	// String server = findServer((long) map.get("user_id"));
	// //Map m = filter(map, "user_id");
	//
	// for (Object o : m.keySet()) {
	// mongoManagers.get(o).put((List<Document>) m.get(o));
	// }
	// mongoManagers.get(server).put(map);
	// }

	Function<Long, String> func = (l) -> {
		Long[] arr = mongoServers.keySet().toArray(new Long[0]);
		for (int i = 0; i < arr.length; i++) {
			if (l >= arr[i] && i == arr.length - 1) {
				return (mongoServers.get(arr[i]));
			} else if (l >= arr[i] && l < arr[i + 1]) {
				return (mongoServers.get(arr[i]));
			}
		}
		return null;
	};

	BiFunction<Map<String, Object>, String, String> server1 = (map, value) -> {
		return findServer((long) map.get(value));
	};

}
