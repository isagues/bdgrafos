package ar.edu.itba.grafos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Neo4JJDBC implements AutoCloseable
{
    private final Connection conn;

    public static void main( String args[] ) throws Exception
    {	// 
        try ( Neo4JJDBC app = new Neo4JJDBC( "jdbc:neo4j:bolt://localhost:7687", "neo4j", "a" ) )
        {
        	// app.listPeople();
        	
        	//  app.listPathPeople();
        	 app.Ej4();
        	
         }
        System.exit(0);
    }

    public Neo4JJDBC( String uri, String user, String password ) throws SQLException
    {
        conn = DriverManager.getConnection(uri, user, password);
        System.out.println(conn);
    }
    
	@Override
	public void close() throws Exception {
		conn.close();
	}
	
    private void listPeople() throws SQLException {
    	String query = 
    			"MATCH (a:Person) "
                + "WHERE a.born > ?  "
                + "RETURN a.name AS myname, a.born "
                + "ORDER BY a.name";
    	try (PreparedStatement stmt = conn.prepareStatement(query)) {
    		stmt.setInt(1, 1960);

    		try (ResultSet rs = stmt.executeQuery()) {
    			while (rs.next()) 
    				System.out.println("Person: "+ rs.getString("myname")+" is "+rs.getInt("a.born"));
    		}
    		
    		
    		// puedo ejecutar con otros parametros.... Tipico de JDBC
    		System.out.println("-----");
    		stmt.setInt(1, 1980);

    		try (ResultSet rs = stmt.executeQuery()) {
    			while (rs.next()) 
    				System.out.println("Person: "+ rs.getString("myname")+" is "+rs.getInt("a.born"));
    		}
    	}
    }	
	
	private void listPathPeople() throws SQLException  {
	 	String query = "MATCH path= (a:Person)-[]->(f:Movie) "
    			+ "	WHERE a.born > $1 "
    			+ "RETURN path "
    			+ "ORDER BY a.name";
    	try (PreparedStatement stmt = conn.prepareStatement(query)) {
    		stmt.setInt(1, 1960);
    		
    		try (ResultSet rs = stmt.executeQuery()) {
    			while (rs.next()) {
    				Object path = rs.getObject("path");
    				System.out.println("Path: "+ path);
    				
    				ArrayList<Object> aaa = (ArrayList) path;
    		
    				for(Object  subelement: aaa) {
    					LinkedHashMap<String, Object> a = (LinkedHashMap<String, Object>) subelement;
    					System.out.println(a.keySet());
    					System.out.println(a.values());
    			//		System.out.println(subelement);
    				}
    		
    			}
    		}
    	}
    }	
	 
    private void Ej4() throws SQLException {
		String query = 
			"MATCH (a:Movie) " +
			"WHERE a.title STARTS WITH ? " +
				"AND size(a.title) > ? " + 
			"RETURN a.title";

    	try (PreparedStatement stmt = conn.prepareStatement(query)) {
    		
			final List<String> titles = List.of("A", "B", "C", "D", "E", "F", "G", "Q", "S");
			stmt.setInt(2, 4);
			
			for(var t : titles) {
				stmt.setString(1, t);
	
				try (ResultSet rs = stmt.executeQuery()) {
					while (rs.next()) 
						System.out.println("Movie: "+ rs.getString("a.title"));
				}
			}
    	}
	}
}