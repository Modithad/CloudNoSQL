package fr.lri.cloudnosql.db.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.Function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Neo4jManager {
	private Driver driver;
	private Session session;
	
	public Neo4jManager(){
		this.driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "secret"));
		this.session = driver.session();
	}
	
	public List<Document> getUsers(){
		String cmd = "MATCH (n:user) RETURN n ";
		StatementResult result = session.run(cmd);
		ArrayList<Long> userIds = new ArrayList<Long>();
		
//		while (result.hasNext()) {
//			Record record = result.next();
//			//userIds.add(record.get("n.user_id").asLong());
//			Gson gson = new GsonBuilder().create();
//			String json = gson.toJson(record.get("n").asMap());
//			System.out.println(json);
//		}
		Function<Record, Document> funcEmpToString = (Record e) -> {
			return new Document(e.get("n").asMap());
		};
		List<Document> l= result.list(funcEmpToString);

		Gson gson = new GsonBuilder().create();
		for (Map<String, Object> map : l) {
		System.out.println(gson.toJson(map));
		}
		return l;
	}
}
