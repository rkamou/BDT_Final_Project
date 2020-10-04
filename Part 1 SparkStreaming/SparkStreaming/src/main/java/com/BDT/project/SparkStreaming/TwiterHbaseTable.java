package com.BDT.project.SparkStreaming;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class TwiterHbaseTable
{

	private static final String TABLE_NAME = "TwiterHbaseTable";
	private static final String CF_DEFAULT = "Tweet_Data";

 public static void reinitDB() {
	 Configuration config = HBaseConfiguration.create(); 

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			if (admin.tableExists(table.getTableName()))
				{
					admin.disableTable(table.getTableName());
					admin.deleteTable(table.getTableName());
				}
				admin.createTable(table);
				
				System.out.println("DataBase Creation Done!");
            
        }catch (Exception e){
            
        }
		
 }
	
	public static void saveTweet (String tweet, Integer score) throws IOException
	{
// 		Creation table	
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

			

//			if (admin.tableExists(table.getTableName()))
//			{
//				admin.disableTable(table.getTableName());
//				admin.deleteTable(table.getTableName());
//			}
//			admin.createTable(table);
			if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
				admin.createTable(table);
				System.out.print("Creating table.... ");
			}
//			Insertion of Data 
			
			HTable firstTable = new HTable(config, TABLE_NAME);
			Put p = new Put(Bytes.toBytes(LocalDateTime.now().toString()));
			p.add(Bytes.toBytes(CF_DEFAULT),Bytes.toBytes("KEY WORDS"),Bytes.toBytes(tweet));
			p.add(Bytes.toBytes(CF_DEFAULT),Bytes.toBytes("SCORE_SENTIMENT"),Bytes.toBytes(String.valueOf(score)));
			
			firstTable.put(p);
			firstTable.put(p);
			
			System.out.println("data inserted");
			
		      // closing HTable
	        firstTable.close();
			
			System.out.println(" Done!");
		}
	}
}