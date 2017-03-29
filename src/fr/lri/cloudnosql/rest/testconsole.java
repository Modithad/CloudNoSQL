package fr.lri.cloudnosql.rest;

import fr.lri.cloudnosql.db.mongo.MongoManager;
import fr.lri.cloudnosql.db.neo4j.Neo4jManager;

public class testconsole {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Neo4jManager m = new Neo4jManager();
		MongoManager mon = new MongoManager("localhost", "mydb");
		mon.put(m.getUsers());
		
	}

}
