package com.example.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;



public class SimpleClient {
   private Cluster cluster;
   private Session session;
   public Session getSession() {
	      return this.session;
	   }
      
   public void connect(String node, String username, String password, String dc) throws Exception {
	  try {
		QueryLogger queryLogger;
		cluster = Cluster.builder()
	            .addContactPoint(node)
                    //.addContactPoint("52.90.124.167")  //DC1
                    //.addContactPoint("35.174.12.21")   //DC2
                    //.addContactPoint("34.239.44.145")  //DC3
                    .withAuthProvider(new PlainTextAuthProvider(username, password))
                    .withLoadBalancingPolicy(
                    		                 DCAwareRoundRobinPolicy.builder()
                    		                 .withLocalDc(dc)
                    		                 .withUsedHostsPerRemoteDc(2)
                    		                 .allowRemoteDCsForLocalConsistencyLevel()
                    		                 .build()
                    		                )
                    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setDefaultIdempotence(true))
                    .build();
		queryLogger = QueryLogger.builder().withConstantThreshold(5).build();
        cluster.register(queryLogger);
           System.out.printf("DataStax Java Driver: %s\n", Cluster.getDriverVersion());
	   System.out.printf("Directing traffic to: %s\n", dc);	  
           Metadata metadata = cluster.getMetadata();
           System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
           for (Host host : metadata.getAllHosts()) {
               System.out.printf("Datatacenter: %-16s; Host: %-16s; Rack: %s; Cassandra %s\n", host.getDatacenter(), host.getAddress(), host.getRack(), host.getCassandraVersion());
               }
           session = cluster.connect();
	   
           System.out.print("\n");
	  } catch (AuthenticationException ae) {
		  System.out.printf("ERROR: Username and/or password are incorrect\n");
		  System.exit(1);
	  }     
   }
   
   public String asciiToHex(String s){
       char[] chars = s.toCharArray();
       StringBuffer hex = new StringBuffer();
       for (int i = 0; i < chars.length; i++)
       {
           hex.append(Integer.toHexString((int) chars[i]));
       }
       return "0x" + hex.toString();
   }
     
   public String  removeChar(String s, char c){
	   StringBuffer buf = new StringBuffer(s.length());
       buf.setLength(s.length());
       int current = 0;
       for (int i=0; i<s.length(); i++){
           char cur = s.charAt(i);
           if(cur != c) buf.setCharAt(current++, cur);
       }
       return buf.toString();   
   }
   
   public String hex2ascii(String s) {
	    String output = new String();
	    for (int i = 2; i < s.length(); i+=2) {
	        String str = s.substring(i, i+2);
	        output= output + (char)Integer.parseInt(str, 16);
	    }
	    return output;
	}
       
   public ArrayList<String> loadFileList(String file) throws IOException{
	   Scanner s = new Scanner(new File(file));
	   ArrayList<String> list = new ArrayList<String>();
	   while (s.hasNext()){
	       list.add(s.next());
	   }
	   s.close();
	   return list;
   }
   
   public String pickItemFromList(ArrayList<String> list) {
	   //TO DO
	   String item;
	   int    size;
	   int    index;
	   Random randomGenerator;
	   randomGenerator = new Random();
	   size = list.size();
	   index = randomGenerator.nextInt(size);
	   item = list.get(index);
	   return item;
   }
   
     
   public ArrayList<UUID> loadResultsList(ResultSet rs, String col) {
	   ArrayList<UUID> list = new ArrayList<UUID>();
	   for (Row row : rs) {   
		   list.add(row.getUUID(col));
	   }
	   return list;
   }
   
   public UUID pickUuidFromList(ArrayList<UUID> list) {
	   //TO DO
	   UUID item;
	   int    size;
	   int    index;
	   Random randomGenerator;
	   randomGenerator = new Random();
	   size = list.size();
	   index = randomGenerator.nextInt(size);
	   item = list.get(index);
	   return item;
   }

	public int randomNumber(int len) {
        Random rand = new Random();
        return rand.nextInt(len);
   }


	public void constantLoad (int noTests, String dc) {
	   String Keyspace      = "tester";
	   String Table         = "test" + "_" + dc;
	   String insertQuery   = "INSERT INTO "   + Keyspace + "." + Table + " (id, something) VALUES ( ?, ?);";
	   String readQuery     = "SELECT * FROM " + Keyspace + "." + Table + " WHERE id = ? ";
       PreparedStatement ps1 = session.prepare(insertQuery);
       PreparedStatement ps2 = session.prepare(readQuery);
       BoundStatement bs = null;
       session.execute(new SimpleStatement("TRUNCATE TABLE " + Keyspace + "." + Table + ";"));
       System.out.print("Executing " + noTests + " read/write operations...\n");
       for (int i = 1; i <= noTests; i+=1) {
           bs = ps1.bind(i, dc + " garbage");
           session.execute(bs);
           bs = ps2.bind(randomNumber(i));
           session.execute(bs);
       }
       System.out.print("Completed!\n");
   }
   
   //
   // TESTS END HERE
   //
   
   public void close() {
	  String clusterName = cluster.getMetadata().getClusterName(); 
      cluster.close();
      System.out.printf("\nDisconnected from cluster: %s\n", clusterName);      
   }

   public static class AddressTests {
      public static int getVersion(InetAddress ia) {
             byte[] address = ia.getAddress();
             if (address.length == 4) return 4;
             else if (address.length == 16) return 6;
             else return -1;
      }      
   }    

   public static void main(String[] args) throws Exception {	   	   
	  //InetAddress address = null;
	  String address = null;
	  String username = "cassandra";
	  String password = "cassandra";
	  String dc       = "DC1";
	  int    noTests    = 10;
	  try {
		  if (args.length > 0 ) {
			  address = args[0];
			  //address = InetAddress.getByName(args[0]);			  
			  if (args.length > 1 ) username = args[1];
			  if (args.length > 2 ) password = args[2];
			  if (args.length > 3 ) noTests  = Integer.parseInt(args[3]);
			  if (args.length > 4 ) dc       = args[4];
		  } else { 
			  address = "127.0.0.1";
		  }
	      System.out.println("Connecting to: " + address);       
	  } catch (Exception e) {
	       System.out.println("ERROR: Could not resolve: " + args[0]);
	       e.printStackTrace();
	       System.exit(1);
      }       
	  SimpleClient client = new SimpleClient();
	  try {
	       //ADD TEST(S) BELOW HERE 
	       client.connect(address, username, password, dc);
	       client.constantLoad(noTests, dc);
	       //ADD TEST(S) ABOVE HERE 
	  } catch (Exception e){
		   System.out.println("ERROR: Cannot connect to cluster\n");
		   e.printStackTrace();
		   System.exit(1);
	  }
      client.close();
   }
}
