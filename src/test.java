/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 */

import java.io.IOException;
import java.math.BigInteger;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.hbase.async.Bytes;
import org.hbase.async.*;

public class test {

public static void getSize(Scan s1,String st, String en) throws IOException
	    {
	    	Scan s = new Scan();
	    	if(st != null) {
		    	 byte[] stb1 = org.apache.commons.codec.binary.Base64.decodeBase64(st);
		    	// LOG.info("  Start row in------   " +Bytes.toStringBinary(stb1));
		    	 s.setStartRow(stb1);
	    	}
	    	if(en != null) {
	    		byte[] enb2 = org.apache.commons.codec.binary.Base64.decodeBase64(en);
	    		//LOG.info("  End row in ------   " +Bytes.toStringBinary(enb2));
	    		s.setStopRow(enb2);
	    	}
	    	
	    	System.out.print("\n Here \n");  
	        Configuration conf = HBaseConfiguration.create();
	        HTable table = new HTable(conf, "tsdb");        
	    	//HTable tt  = new HTable(HBaseConfiguration.create(), "tsdb");			
	    	ResultScanner ss = table.getScanner(s);
	    	int col = 0;
	    	int size = 0;
	    	for(Result r:ss){	
	    		col = 0;
	            for(KeyValue kv : r.raw()){ 
	            	 col = col+kv.getLength(); 
	            	 System.out.print("\n Length keyValue " +kv.getLength() + "\n");    
	            }	
	            size = size + col/1000;
	        }
	    	System.out.print("\n Size in " +size + "\n");   
	    }
	
public static void main(String[] args)  throws Exception {
	
	DataType dt = new DataType();
	dt.setHbaseClient("haisen24.ux.uis.no");
	String[] tagk = {"1","host"};
    String[] tagv = {"1","*"};
    
	String[] val =  dt.getRowkeyFilter("2012/11/07-09:00:00","2012/11/08-10:00:00", "cipsi.haisen.proc.loadavg.1m",  tagk, tagv);
	System.out.println("\n Filter  ======" + org.apache.hadoop.hbase.util.Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(val[2])));
	//System.out.println("\n================ " +org.apache.hadoop.hbase.util.Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64("AAABUYoUEg==")));
	//System.out.println("\n================ " +org.apache.hadoop.hbase.util.Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(val[1])));
	//System.out.println("\n================ " +org.apache.hadoop.hbase.util.Bytes.toStringBinary(org.apache.hadoop.hbase.util.Bytes.toBytes(val[2])));
	dt.getBaseTimestamp(org.apache.commons.codec.binary.Base64.decodeBase64(val[0]));
	dt.getBaseTimestamp(org.apache.commons.codec.binary.Base64.decodeBase64(val[1]));
	
	
	Scan scans = new Scan();
	//getSize (scans,val[0],val[1]);
	
    scans.setStartRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[0]));
    scans.setStopRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[1]));    
    
    RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
           new RegexStringComparator( org.apache.hadoop.hbase.util.Bytes.toString(org.apache.commons.codec.binary.Base64.decodeBase64(val[2]))));
   scans.setFilter(rowFilterRegex); 

   Configuration conf = HBaseConfiguration.create();
   conf.set("hbase.zookeeper.quorum", "haisen24.ux.uis.no");
   HTable table1 = new HTable(conf, "tsdb");
   ResultScanner ss = table1.getScanner(scans);
  // Result result = null;   
   int rowcount1 = 0;
   byte [] TS = new byte[4];
   for(Result r:ss){	
	   // rowcount1++;
	   //System.out.print("\n Rowkey " +  org.apache.hadoop.hbase.util.Bytes.toStringBinary(result.getRow())  ); 
  	   rowcount1++;
  	   System.arraycopy(r.getRow(), 3, TS, 0, 4); 
  	   System.out.print("\n Rowkey " +  org.apache.hadoop.hbase.util.Bytes.toInt(TS)  );
       for(KeyValue kv : r.raw()){ 
       	   //col = col+kv.getLength(); 
       	   //System.out.print("\n Length keyValue " +kv.getLength() + "\n");   
    	   //System.out.print("\n column qualifier " + org.apache.hadoop.hbase.util.Bytes.toString(kv.getQualifier() )+ "\n"); 
    	   byte[] qual = kv.getQualifier();
    	   final int len = qual.length;
    	   //final short delta = (short) ((Bytes.getShort(qual) & 0xFFFF) >>> 4); // this is 12 bit delta qualifier.
    	   
    	   //final short delta = (short) ((Bytes.getShort(kv.getQualifier()) & 0xFFFF) >>> 4);
    	   final short delta = (short) (( org.apache.hadoop.hbase.util.Bytes.toShort(kv.getQualifier()) & 0xFFFF) >>> 4);
    	   //byte[] b = Bytes.fromShort(delta1);
    	   //get.qualifier(kv.getQualifier());
    	   int val_len = 1;
    	   final byte[] v = kv.getValue();
           //val_len += floatingPointValueToFix(qual[1], v) ? 4 : v.length;
    	  // System.out.print("\n Delta length " + len + "\n");   
    	   //System.out.print("\n Delta index 1 " + delta + " index 0 -> " + qual[0] +  "\n");  
    	   //System.out.print("\n Delta bytes " + qual + "\n");  
    	   //System.out.print("\n Delta qualifier " + delta + "\n");   
    	   //System.out.print("\n Delta qualifier 1=> " + delta + "\n"); 
    	   System.out.print("\n Value " + org.apache.hadoop.hbase.util.Bytes.toFloat(kv.getValue() )+ "\n");   
    	   rowcount1++;
       }	
      // size = size + col/1000;
   }
   System.out.print("\n rowcount1 " +  rowcount1  ); 
 /*  while ((result = ss.next())!=null)
    {
   	   System.out.print("\n Value " +  org.apache.hadoop.hbase.util.Bytes.toFloat(result.value())  ); 
   	   //System.out.print("\n Value " +  org.apache.hadoop.hbase.util.Bytes.toFloat(result.value())  ); 
   	   rowcount1++;
   	   //System.out.print("\n Rowkey " +  org.apache.hadoop.hbase.util.Bytes.toInt(TS)  );
    }*/

    //System.out.println("\n Row count ================ " +rowcount1);

	/*TSDB tsdb = new TSDB(new HBaseClient("localhost"),"tsdb","tsdb-uid");
	 final String METRICS_QUAL = "metrics";
	 final short METRICS_WIDTH = 3;
	 final String TAG_NAME_QUAL = "tagk";
	 final short TAG_NAME_WIDTH = 3;
	 final String TAG_VALUE_QUAL = "tagv";
	 final short TAG_VALUE_WIDTH = 3;
	 final byte[] uidtable = "tsdb-uid".getBytes();
	 HBaseClient client = new HBaseClient("localhost");
	 UniqueId metrics = new UniqueId(client, uidtable, METRICS_QUAL, METRICS_WIDTH);
	    
	 System.out.println("\n================ " +metrics.toString());
	
	//getHexSplits(org.apache.hadoop.hbase.util.Bytes.toString(org.apache.hadoop.hbase.util.Bytes.toBytes(val[0])),org.apache.hadoop.hbase.util.Bytes.toString(org.apache.hadoop.hbase.util.Bytes.toBytes(val[1])) , 4); 
	/*
	String sdate ="2013/05/08 06:00:00";
	@SuppressWarnings("deprecation")
	Date d1 = new Date(sdate);    
	long timestamp = d1.getTime()/1000;
   
	String edate="2013/05/08 08:00:00";
	@SuppressWarnings("deprecation")
	Date end_date = new Date(edate);
	long end_timestamp = end_date.getTime()/1000;
	
	DataType.setStartTime( timestamp);	
	 
	DataType.setEndTime(end_timestamp);
	long  end_time = DataType.getEndTime();
	final byte[] start_row = new byte[3 + Const.TIMESTAMP_BYTES];
	final byte[] end_row = new byte[3 + Const.TIMESTAMP_BYTES];
	//final Scanner scanner = tsdb.client.newScanner("tsdb");
	 
	final short metric_width = Const.METRICS_BYTES;	
	 
	TSDB tsdb = new TSDB(new HBaseClient("localhost"),"tsdb","tsdb-uid");

	
	
	 org.hbase.async.Bytes.setInt(start_row, (int) DataType.getScanStartTime(), metric_width);
	 org.hbase.async.Bytes.setInt(end_row, (end_time == -1
             ? -1 // Will scan until the end (0xFFF...).
             : (int) DataType.getScanEndTime()),
   3);
	 
	String metricsval="proc.loadavg.1m";
	 @SuppressWarnings("deprecation")
	HBaseConfiguration config = new HBaseConfiguration();
	 HTable table = new HTable(config, "tsdb-uid");
	 Get g = new Get(org.apache.hadoop.hbase.util.Bytes.toBytes(metricsval));
	 Result r = table.get(g);
	 byte [] metric = r.getValue(org.apache.hadoop.hbase.util.Bytes.toBytes("id"),
			 org.apache.hadoop.hbase.util.Bytes.toBytes("metrics"));
	 System.arraycopy(metric, 0, start_row, 0, 3);
	 System.arraycopy(metric, 0, end_row, 0, 3); 
	 
	 
	 final HBaseClient hbase = new HBaseClient("localhost");
	 final Scanner scanner = hbase.newScanner("tsdb");
	 scanner.setStartKey(start_row);
	 scanner.setStopKey(end_row);
	 System.out.println("\n================ " +org.apache.hadoop.hbase.util.Bytes.toStringBinary(start_row));
	 System.out.println("\n================ " +org.apache.hadoop.hbase.util.Bytes.toStringBinary(end_row));
	
	 DataType dt = new DataType();
	 Map<String, String> map = new HashMap<String, String>();
	 //map.put("host", "pc-0-227");
	 //map.put("host", "foo");
	 //map.put("host", "bikash|pc-0-227"); // tag key and tag value 
	 map.put("host", "*");
	 
	 
	 dt.setTimeSeries("proc.loadavg.1m",map,tsdb);
	 dt.createAndSetFilter(scanner,tsdb);

	 //System.out.println("Scanning table... ");
	 String [] startrowkey = new String[32];
     int rowcount=0;
     int kvcount =0;
     String lastr = null;
     ArrayList<ArrayList<KeyValue>> rows;
     while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
       for (final ArrayList<KeyValue> row : rows) {
         rowcount++;
         // Take a copy of the row-key because we're going to zero-out the
         // time-stamp and use that as a key in our `seen' map.
         final byte[] key = row.get(0).key().clone();        
         for (final KeyValue kv : row) {
             kvcount++; 
             byte[] rowkey = kv.key();
             byte[] value = kv.value();
             final byte[] qual = kv.qualifier();
             startrowkey[rowcount] = org.apache.commons.codec.binary.Base64.encodeBase64String(rowkey);
             //System.arraycopy(result.getRow(), 3, rowArr, 0, 4);
             lastr = org.apache.hadoop.hbase.util.Bytes.toStringBinary(rowkey);
             
         }
       }
     }
     
     //System.out.println("\n Row count ================ " +rowcount);
  
     byte [] ts1 = new byte[4];
     System.arraycopy(org.apache.commons.codec.binary.Base64.decodeBase64(startrowkey[rowcount]), 3, ts1, 0, 4); 

     String StartRow = null;
     int  rowcount1 =0;
     byte [] rowArr = new byte[6];
     byte [] rowArr1 = new byte[6];
	 HTable table1 = new HTable(config, "tsdb");
     Scan scans = new Scan();
     scans.setStartRow(org.apache.commons.codec.binary.Base64.decodeBase64(startrowkey[1]));
     scans.setStopRow(org.apache.commons.codec.binary.Base64.decodeBase64(startrowkey[rowcount]));
     byte[] Rows1 = org.apache.commons.codec.binary.Base64.decodeBase64(startrowkey[1]);
     System.arraycopy(Rows1, 7, rowArr, 0, 6);
     byte[] Rows2 = org.apache.commons.codec.binary.Base64.decodeBase64(startrowkey[rowcount]);
     System.arraycopy(Rows1, 7, rowArr1, 0, 6);
  
    StringBuilder filter1 = dt.createAndSetFilter1(tsdb);
    RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator( filter1.toString()));
    scans.setFilter(rowFilterRegex); 

    
    //KeyRegexp filter = new KeyRegexp(filter1.toString(), CHARSET);
    //scans.setFilter(filter);
    ResultScanner ss = table1.getScanner(scans);
    Result result = null;
    StartRow = org.apache.hadoop.hbase.util.Bytes.toStringBinary(ss.next().getRow());
    byte [] TS = new byte[4];
    while ((result = ss.next())!=null)
     {
    	 //System.out.print("\n Rowkey " +  org.apache.hadoop.hbase.util.Bytes.toStringBinary(result.getRow())  ); 
    	 rowcount1++;
    	 System.arraycopy(result.getRow(), 3, TS, 0, 4); 
    	 //System.out.print("\n Rowkey " +  org.apache.hadoop.hbase.util.Bytes.toInt(TS)  );
     }

     System.out.println("\n Row count ================ " +rowcount1);
     */

}


public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
	  byte[][] splits = new byte[numRegions-1][];
	  BigInteger lowestKey = new BigInteger(startKey, 16);
	  BigInteger highestKey = new BigInteger(endKey, 16);
	  BigInteger range = highestKey.subtract(lowestKey);
	  BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
	  lowestKey = lowestKey.add(regionIncrement);
	  for(int i=0; i < numRegions-1;i++) {
	    BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
	    byte[] b = String.format("%016x", key).getBytes();
	    splits[i] = b;
	  }
	  return splits;
	}
}
