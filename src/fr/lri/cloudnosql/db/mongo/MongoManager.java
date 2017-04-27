package fr.lri.cloudnosql.db.mongo;

import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import fr.lri.cloudnosql.db.meta.DBType;
import fr.lri.cloudnosql.db.meta.IDBManager;

public class MongoManager implements IDBManager {
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

	public Object get(Map<String, Object> map) {

		return userCollection.find(new Document(map)).first();

	}

	public List<Object> getIn(String field, List<?> list) {
		DBObject obj = new BasicDBObject(field, new BasicDBObject("$in", list));
		return Lists.newArrayList(userCollection.find((Bson) obj));
	}

	@Override
	public DBType getDBType() {
		// TODO Auto-generated method stub
		return DBType.MONGO_DB;
	}

}
