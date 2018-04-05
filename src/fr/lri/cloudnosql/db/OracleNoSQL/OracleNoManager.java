package fr.lri.cloudnosql.db.OracleNoSQL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import fr.lri.cloudnosql.db.meta.DBType;
import fr.lri.cloudnosql.db.meta.IDBManager;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.util.SerializationUtil;

public class OracleNoManager implements IDBManager {
	private KVStore store;
	public String hname;

	public OracleNoManager(String storeName, String hostName, String hostPort) {
		hname = hostName;
		this.setStore(KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort)));
	}

	public KVStore getStore() {
		return store;
	}

	public void setStore(KVStore store) {
		this.store = store;
	}

	public void put(String majorKey, Object majorValue, Map<String, Object> map) {

		List<Operation> batch = new LinkedList<Operation>();
		List<String> majorPath = Arrays.asList(majorKey, majorValue.toString());
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			Key key = Key.createKey(majorPath, entry.getKey());
			Value value = Value.createValue(SerializationUtil.getBytes(entry.getValue()));
			Operation op = store.getOperationFactory().createPut(key, value);
			batch.add(op);
		}

		try {
			store.execute(batch);
		} catch (OperationExecutionException | FaultException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void put(String majorKey, String minorKey, Object value) {
		Key k = Key.createKey(majorKey, minorKey);
		Value v = Value.createValue(SerializationUtil.getBytes(value));
		store.put(k, v);
	}

	public void put(String key, Object val) {
		System.out.println("inside oracle");
		System.out.println(key);
		Key k = Key.createKey(key);
		Value v = Value.createValue(SerializationUtil.getBytes(val));
		store.put(k, v);
	}

	public void put(String majorKey, String minorKey, Map<String, Object> map) {
		Key k = Key.createKey(majorKey, minorKey);
		Value v = Value.createValue(SerializationUtil.getBytes(map));
		store.put(k, v);
	}

	public Object get(String key) {

		// System.out.println(store.get(Key.createKey("user_id", key)));
		Object o = store.get(Key.createKey(key));
		if (o == null)
			return null;
		else
			return SerializationUtil.getObject((store.get(Key.createKey(key)).getValue().getValue()), Object.class);

	}

	public Object get(String majorKey, String minorKey) {
		if (store.get(Key.createKey(majorKey, minorKey)) == null) {
			return null;
		} else
			return SerializationUtil.getObject((store.get(Key.createKey(majorKey, minorKey)).getValue().getValue()),
					Object.class);
	}

	// public Object get(String key, String value) {
	//
	// Object o= store.get(Key.createKey(key, value));
	// System.out.println(store.get(Key.createKey(key, value)));
	// return
	// SerializationUtil.getObject((store.get(Key.createKey(key)).getValue().getValue()),
	// Object.class);
	//
	// }

	public Object getMulti(String key) {

		final Map<Key, ValueVersion> valueVersion = store.multiGet(Key.createKey(key), null, null);
		List<Object> list = new ArrayList<>();
		for (Entry<Key, ValueVersion> elm : valueVersion.entrySet()) {
			// System.out.print(elm.getKey() + "-->");
			list.add(SerializationUtil.getObject(elm.getValue().getValue().getValue(), Object.class));
		}

		return list;

	}

	@Override
	public DBType getDBType() {
		// TODO Auto-generated method stub
		return DBType.ORACLE_NOSQL;
	}
}
