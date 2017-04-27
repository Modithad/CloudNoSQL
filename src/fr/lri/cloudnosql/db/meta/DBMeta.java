package fr.lri.cloudnosql.db.meta;

import java.util.HashSet;
import java.util.Set;

public class DBMeta {
	private String Entity;
	private DBType DBType;
	private String DBName;
	private String Collection;
	private Set<String> distKeys;
	private Set<String> primKeys;
	private Set<String> reverseKeys;

	public DBMeta() {
		distKeys = new HashSet<>();
		primKeys = new HashSet<>();
		reverseKeys = new HashSet<>();
	}

	public void addDistKey(String k) {
		distKeys.add(k);
	}

	public void addPrimKey(String k) {
		primKeys.add(k);
	}

	public void addReverseKey(String k) {
		reverseKeys.add(k);
	}

	public Set<String> getReverseKeys() {
		return reverseKeys;
	}

	public void setReverseKeys(Set<String> reverseKeys) {
		this.reverseKeys = reverseKeys;
	}

	public Set<String> getDistKeys() {
		return distKeys;
	}

	public void setDistKeys(Set<String> distKeys) {
		this.distKeys = distKeys;
	}

	public Set<String> getPrimKeys() {
		return primKeys;
	}

	public void setPrimKeys(Set<String> primKeys) {
		this.primKeys = primKeys;
	}

	public String getEntity() {
		return Entity;
	}

	public void setEntity(String entity) {
		Entity = entity;
	}

	public DBType getDBType() {
		return DBType;
	}

	public void setDBType(DBType dBType) {
		DBType = dBType;
	}

	public String getDBName() {
		return DBName;
	}

	public void setDBName(String dBName) {
		DBName = dBName;
	}

	public String getCollection() {
		return Collection;
	}

	public void setCollection(String collection) {
		Collection = collection;
	}

}
