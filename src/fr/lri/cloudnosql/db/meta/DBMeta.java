package fr.lri.cloudnosql.db.meta;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class DBMeta {
	private String Entity;
	private DBType DBType;
	private String DBName;
	private String Collection;
	private LinkedList<String> distKeys;
	private LinkedList<String> primKeys;
	private LinkedList<String> reverseKeys;
	private int level;

	public DBMeta() {
		distKeys = new LinkedList<>();
		primKeys = new LinkedList<>();
		reverseKeys = new LinkedList<>();
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

	public LinkedList<String> getReverseKeys() {
		return reverseKeys;
	}

	public void setReverseKeys(LinkedList<String> reverseKeys) {
		this.reverseKeys = reverseKeys;
	}

	public LinkedList<String> getDistKeys() {
		return distKeys;
	}

	public void setDistKeys(LinkedList<String> distKeys) {
		this.distKeys = distKeys;
	}

	public LinkedList<String> getPrimKeys() {
		return primKeys;
	}

	public void setPrimKeys(LinkedList<String> primKeys) {
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

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

}
