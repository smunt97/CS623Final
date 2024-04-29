import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class CS623Final {

    
public static void main(String args[]) throws ClassNotFoundException, SQLException 
    	{
			//Load Postgresql driver
    		Class.forName("org.postgresql.Driver");
    		
    		//Connect to the default database with credentials
    		Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "12345Lala_");
    		con.setAutoCommit(false);
    		
    		//For Atomicity
    		Statement stmt = null;
    		
    		// For isolation
    		con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    		
    		
    		
    		try {
    			// Create statement object
    			stmt = con.createStatement();
    			
    		//
    		 	//Create Table product
    			stmt.executeUpdate("CREATE TABLE product(prodid VARCHAR(5), pname Char(20), price Decimal)");
    		 	
    			//Add primary key
    			stmt.executeUpdate("ALTER TABLE product ADD CONSTRAINT pk_prod PRIMARY KEY (prodid)");
    			
    			//Insert Values
    			stmt.executeUpdate("INSERT INTO product VALUES ('p1', 'tape', 2.5)");
    			stmt.executeUpdate("INSERT INTO product VALUES ('p2', 'tv', 250)");
    			stmt.executeUpdate("INSERT INTO product VALUES ('p3', 'ver', 80)");
    			
    			
    			//Create Table depot
    			stmt.executeUpdate("CREATE TABLE depot(depid VARCHAR(5), addr Char(20), volume INT)");
    			
    			//Add primary key
    			stmt.executeUpdate("ALTER TABLE depot ADD CONSTRAINT pk_depot PRIMARY KEY (depid)");
    			
    			//Insert values
    			stmt.executeUpdate("INSERT INTO depot VALUES ('d1', 'NEW YORK', 9000)");
    			stmt.executeUpdate("INSERT INTO depot VALUES ('d2', 'Syracuse', 6000)");
    			stmt.executeUpdate("INSERT INTO depot VALUES ('d4', 'NEW YORK', 2000)");
    			
    			//Create table stock
    			stmt.executeUpdate("CREATE TABLE stock(prodid VARCHAR(5), depid VARCHAR(5), Quantity INT)");
    			
    			//Add primary Key
    			stmt.executeUpdate("ALTER TABLE stock ADD CONSTRAINT pk_stock PRIMARY KEY (prodid,depid)");
    			
    			//Add Foreign key
    			stmt.executeUpdate("ALTER TABLE stock ADD CONSTRAINT fk_stockprod FOREIGN KEY (prodid) REFERENCES product(prodid) ON DELETE CASCADE");
    			stmt.executeUpdate("ALTER TABLE stock ADD CONSTRAINT fk_stockdep FOREIGN KEY (depid) REFERENCES depot(depid) ON DELETE CASCADE");
    			
    			//Insert values
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p1', 'd1', 1000)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p1', 'd2', -100)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p1', 'd4', 1200)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p3', 'd1', 3000)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p3', 'd4', 2000)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p2', 'd4', 1500)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p2', 'd1', -400)");
    			stmt.executeUpdate("INSERT INTO stock VALUES ('p2', 'd2', 2000)");
    			
    		}
    		catch (SQLException e) {
    			System.out.println("An exception was thrown");
    			
    			//For atomicity
    			con.rollback();
    			stmt.close();
    			con.close();
    			return;
    		}
    		
    		
    		
    		//Deleting product p1
    			
    		try {
    			stmt = con.createStatement();

    			System.out.println("---BEFORE DELETION---");
    			ResultSet rs = stmt.executeQuery("Select * from product");
    			while(rs.next()) {
    				System.out.println("#prod:" + rs.getString("prodid")  +"\tproductname:"  +rs.getString ("pname") + "\tprice:" +rs.getDouble("price"));
        				
    			}
    			System.out.println();
    			ResultSet rss = stmt.executeQuery("Select * from depot");
    			while(rss.next()) {
    				System.out.println("#dep:" +rss.getString("depid") +"\taddress:" +rss.getString("addr") + "\tVolume:" +rss.getInt("volume"));
    			}
    			
    			System.out.println();
    			ResultSet rs1 = stmt.executeQuery("Select * from stock");
    			while(rs1.next()) {
    				System.out.println("#prod:" + rs1.getString("prodid")  +"\tdepid:"  +rs1.getString ("depid") + "\tQuantity:" +rs1.getInt("Quantity"));
        				
    			}
    			
    			
    			System.out.println();
    			System.out.println("---AFTER DELETION of prodid p1---");
    			stmt.executeUpdate("Delete FROM product WHERE prodid = 'p1'");
    			ResultSet rs2 = stmt.executeQuery("Select * from product");
    			while(rs2.next()) {
    				System.out.println("#prod:" + rs2.getString("prodid")  +"\tproductname:"  +rs2.getString ("pname") + "\tprice:" +rs2.getDouble("price"));
        				
    			}
    			System.out.println();
    			ResultSet rs3 = stmt.executeQuery("Select * from stock");
    			while(rs3.next()) {
    				System.out.println("#prod:" + rs3.getString("prodid")  +"\tdepid:"  +rs3.getString ("depid") + "\tQuantity:" +rs3.getInt("quantity"));
        				
    			}
    		}

    			catch (SQLException e) {
        			System.out.println("An exception was thrown");
        			
        			//For atomicity
        			con.rollback();
        			stmt.close();
        			con.close();
        			return;
        		}
        		
    		
    		//deleting depid = d1
    		
    		try {
    				
    			stmt = con.createStatement();

    			System.out.println();
    			System.out.println("---AFTER DELETION of depid d1---");
    			stmt.executeUpdate("Delete FROM depot WHERE depid = 'd1'");
    			ResultSet rs4 = stmt.executeQuery("Select * from depot");
    			while(rs4.next()) {
    				System.out.println("#dep:" + rs4.getString("depid")  +"\taddress:"  +rs4.getString ("addr") + "\tvolume:" +rs4.getInt ("volume"));
        				
    			}
    			System.out.println();
    			ResultSet rs5 = stmt.executeQuery("Select * from stock");
    			while(rs5.next()) {
    				System.out.println("#prod:" + rs5.getString("prodid")  +"\tdepid:"  +rs5.getString ("depid") + "\tQuantity:" +rs5.getInt("quantity"));
        				
    			}
    					
    			
    		}
    		catch (SQLException e) {
    			System.out.println("An exception was thrown");
    			
    			//For atomicity
    			con.rollback();
    			stmt.close();
    			con.close();
    			return;
    		}
    		con.commit();
    		stmt.close();
    		con.close();
    		
    		
    		
    		
    	
    		System.out.println("connection created!");
    	
    	}

}
		
