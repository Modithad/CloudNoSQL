package fr.lri.cloudnosql.db.OracleNoSQL;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

public class OracleNoClient {
	private KVStore store;

	public OracleNoClient(String storeName, String hostName, String hostPort) {
		this.setStore(KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort)));
	}

	public KVStore getStore() {
		return store;
	}

	public void setStore(KVStore store) {
		this.store = store;
	}
}