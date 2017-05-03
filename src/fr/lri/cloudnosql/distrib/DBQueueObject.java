package fr.lri.cloudnosql.distrib;

import java.util.HashMap;
import java.util.Map;

import fr.lri.cloudnosql.db.meta.IDBManager;

public class DBQueueObject {
	private IDBManager manager;
	private Map<String, Object> query = new HashMap<>();
	private String output;
	public IDBManager getManager() {
		return manager;
	}
	public void setManager(IDBManager manager) {
		this.manager = manager;
	}
	public Map<String, Object> getQuery() {
		return query;
	}
	public void setQuery(Map<String, Object> query) {
		this.query = query;
	}
	public String getOutput() {
		return output;
	}
	public void setOutput(String output) {
		this.output = output;
	}
	
}
