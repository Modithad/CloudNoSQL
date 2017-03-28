package fr.lri.cloudnosql.db.mongo;

import com.mongodb.MongoClient;

public class MongoManager {
	private MongoClient mongoClient;
	
	public MongoManager(String host){
		setMongoClient(new MongoClient(host));
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}
}
