package ar.edu.itba.grafos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class Neo4jRemote implements AutoCloseable {
	
	private final Driver driver;
	public static void main( String args[] ) throws Exception {
		try ( Neo4jRemote app = new Neo4jRemote( "bolt://localhost:7687", "neo4j", "papanata" ) ){
			// app.listPeople();
			// app.Ej2();
			app.Ej5();
		} 
	}
	
	public Neo4jRemote( String uri, String user, String password ) {
		driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) ); 
	}
	
	@Override
	public void close() throws Exception {
		driver.close();
	}
	
	public List<Record> listPeople() {
		try ( Session session = driver.session() ) {
			return session.readTransaction( tx -> { 
				List<Record> dataperson = new ArrayList<>(); 
				Result result = tx.run("MATCH (a:Person) RETURN a.name, a.born ORDER BY a.name" ); 
		
				while ( result.hasNext() ) {
					Record aRec = result.next(); dataperson.add(aRec); System.out.println(aRec);
				}
				return dataperson; 
			});
		} 
	}

	// Query con placeholder
	public void Ej2() {
		
		final String query = 
			"MATCH (a:Movie) " +
			"WHERE a.title STARTS WITH $title " +
				"AND size(a.title) > $len " + 
			"RETURN a.title";
		
		try ( Session session = driver.session() ) {
			session.readTransaction( tx -> { 
				List<Record> dataperson = new ArrayList<>();
				
				final List<String> titles = List.of("A", "B", "C", "D", "E", "F", "G", "Q", "S");

				final Map<String,Object> params = new HashMap<>(); 
				params.put("len", 4);
				
				for(var t : titles) {
					params.put("title",t);
					final Result result = tx.run(query, params); 
		
					while ( result.hasNext() ) {
						Record aRec = result.next(); 
						dataperson.add(aRec); 
						System.out.println(aRec);
					}
				}
				return dataperson; 
			});
		}
	}

	private void Ej5() {
		
		final String query = 
			"MATCH (c:Category)<-[hc:HasCategory]-(p:Product)<-[pu:Contains]-(s:Sales)-[:PurchasedBy]->(cust:Customer), " +
				"(s)-[:HasOrderDate]->(d:Date)" +
			"RETURN cust.CompanyName as Name, " +
				"c.CategoryName as Category, " +
				"d.DayNbYear as Year, " +
				"sum(tofloat(s.SalesAmount)) as Volume " +
			"ORDER BY Name"
			;
		try ( Session session = driver.session() ) {
			session.readTransaction( tx -> { 
				List<Record> data = new ArrayList<>();
				
				final Result result = tx.run(query); 
	
				while ( result.hasNext() ) {
					Record aRec = result.next(); 
					data.add(aRec); 
					System.out.println(aRec);
				}
				System.out.printf("Read %d\n", data.size());
				return data; 
			});
		}
	}
}


