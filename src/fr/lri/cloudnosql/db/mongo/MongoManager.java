package fr.lri.cloudnosql.db.mongo;

import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoManager {
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> userCollection;

	public MongoCollection<Document> getUserCollection() {
		return userCollection;
	}

	public void setUserCollection(MongoCollection<Document> userCollection) {
		this.userCollection = userCollection;
	}

	public MongoManager(String host, String database) {
		setMongoClient(new MongoClient(host));
		mongoDatabase = mongoClient.getDatabase(database);
		userCollection = mongoDatabase.getCollection("testusers");
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public void put(List<Document> documents) {
		// mongoClient.
		userCollection.insertMany(documents);
	}

	public void put(Map<String, Object> map) {
		userCollection.insertOne(new Document(map));
	}

}
