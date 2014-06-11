/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 * 
 * Description: This class is used to get timeseries data from tsdb table.
 */


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
public class Fun {

    private static Log LOG = LogFactory.getLog(Fun.class);
    
    public static long getEndTimeAtResolution(long time, int resolution) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		switch (resolution) {
			case Calendar.DATE:
				cal.set(Calendar.HOUR, 23);
			case Calendar.HOUR:
				cal.set(Calendar.MINUTE, 59);
			case Calendar.MINUTE:
				cal.set(Calendar.SECOND, 59);
			case Calendar.SECOND:
				cal.set(Calendar.MILLISECOND, 999);
			default:
				break;
		}
		
		return cal.getTimeInMillis();
	}

	public static Scan[] generateHexPrefixScans(Calendar startCal, Calendar endCal, String dateFormat, ArrayList<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
		ArrayList<Scan> scans = new ArrayList<Scan>();		
		String[] salts = new String[16];
		for (int i=0; i < 16; i++) {
			salts[i] = Integer.toHexString(i);
		}
		
		SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
		long endTime = getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
		while (startCal.getTimeInMillis() < endTime) {
			int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
			
			for (int i=0; i < salts.length; i++) {
				Scan s = new Scan();
				s.setCaching(caching);
				s.setCacheBlocks(cacheBlocks);
				
				// add columns
				for (Pair<String,String> pair : columns) {
					s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
				}
				//01012310eded859-a6b8-463c-8c8e-721592101231

				s.setStartRow(Bytes.toBytes(salts[i] + String.format("%06d", d)));
				s.setStopRow(Bytes.toBytes(salts[i] + String.format("%06d", d + 1)));
				
				if (LOG.isDebugEnabled()) {
					LOG.info("Adding start-stop range: " + salts[i] + String.format("%06d", d) + " - " + salts[i] + String.format("%06d", d + 1));
				}
				
				scans.add(s);
			}
			
			startCal.add(Calendar.DATE, 1);
		}
		
		return scans.toArray(new Scan[scans.size()]);
	}
    public static Scan[] generateBytePrefixScans(Calendar startCal, Calendar endCal, String dateFormat, ArrayList<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
	ArrayList<Scan> scans = new ArrayList<Scan>();
	
	SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
	long endTime = getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
	
	byte[] temp = new byte[1];
	while (startCal.getTimeInMillis() < endTime) {
	    for (byte b=Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
		int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
		
		Scan s = new Scan();
		s.setCaching(caching);
		s.setCacheBlocks(cacheBlocks);
		// add columns
		for (Pair<String,String> pair : columns) {
                    s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
                }
		
		temp[0] = b;
		s.setStartRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d))));
		s.setStopRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d + 1))));
		if (LOG.isDebugEnabled()) {
                    LOG.info("Adding start-stop range: " + temp + String.format("%06d", d) + " - " + temp + String.format("%06d", d + 1));
                }
		
		scans.add(s);
	    }
	    
	    startCal.add(Calendar.DATE, 1);
	}

	return scans.toArray(new Scan[scans.size()]);
    }
    public static Scan[] generateScans(String st, String en,  ArrayList<Pair<String,String>> columns,int caching, boolean cacheBlocks) {
	ArrayList<Scan> scans = new ArrayList<Scan>();
	Scan s = new Scan();
	s.setCaching(caching);
	s.setCacheBlocks(cacheBlocks);
	if(columns !=null){
	    for (Pair<String,String> pair : columns) {
		String second = pair.getSecond();
		if(second == null)
		    s.addFamily(pair.getFirst().getBytes());
		else
		    s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
	    }
	}
	if(st != null) {
	    	byte[] stb1 = org.apache.commons.codec.binary.Base64.decodeBase64(st);
		s.setStartRow(stb1);
	}
	if(en != null) {
	    byte[] enb2 = org.apache.commons.codec.binary.Base64.decodeBase64(en);
	    s.setStopRow(enb2);
	}
	scans.add(s);
	return scans.toArray(new Scan[scans.size()]);
    }
    
    public static Scan[] generateScansRows(String st, String en, int caching, boolean cacheBlocks,  String filter, int batch) {
    	//LOG.info("  End row------   " +Bytes.toStringBinary( org.apache.commons.codec.binary.Base64.decodeBase64(en)));
    	LOG.info("  cache-----   " + caching);
    	ArrayList<Scan> scans = new ArrayList<Scan>();
    	Scan s = new Scan();
    	//s.setCacheBlocks(false);
    	s.setBatch(batch);
    	s.setCaching(caching); // 1 is the default in Scan, which will be bad for
         // MapReduce jobs
        s.setCacheBlocks(false); // don't set to true for MR jobs
    	if(st != null) {
    	    	byte[] stb1 = org.apache.commons.codec.binary.Base64.decodeBase64(st);
    	    	 LOG.info("  Start row------   " +Bytes.toStringBinary(stb1));
    		s.setStartRow(stb1);
    	}
    	if(en != null) {
    	    byte[] enb2 = org.apache.commons.codec.binary.Base64.decodeBase64(en);
    	    LOG.info("  End row------   " +Bytes.toStringBinary(enb2));
    	    s.setStopRow(enb2);
    	}
    	//LOG.info("  Filter- -----   " +  filter);
    	RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator( Bytes.toString(org.apache.commons.codec.binary.Base64.decodeBase64(filter))));     
    	LOG.info("  Filter------   " +  Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(filter)));
    	s.setFilter(rowFilterRegex);
    	/*try {
			getSize(s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	
    	
    	scans.add(s);
    	return scans.toArray(new Scan[scans.size()]);
        }

    public static void getSize(Scan s) throws IOException
    {
    	//Scan s = new Scan();    	
    	//System.out.print("\n Here \n");  
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
            	// System.out.print("\n Length keyValue " +kv.getLength() + "\n");    
            }	
            size = size + col/1000;
        }
    	LOG.info("\n  Size of HBase block in KB =>   " +  size);
    	//System.out.print("\n Size in " +size + "\n");   
    }
    public static Scan[] generateScansTbl(int caching, boolean cacheBlocks,   int batch) {
    	
    	ArrayList<Scan> scans = new ArrayList<Scan>();
    	Scan s = new Scan();
    	//s.setCacheBlocks(false);
    	s.setBatch(batch);
    	s.setCaching(caching); // 1 is the default in Scan, which will be bad for
         // MapReduce jobs
        s.setCacheBlocks(false); // don't set to true for MR jobs    	
        
    	
    	scans.add(s);
    	return scans.toArray(new Scan[scans.size()]);
        }

}
