import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hbase.async.HBaseClient;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseException;

import net.opentsdb.*;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.Query;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.uid.NoSuchUniqueName;

public class OpenTSDB{


	private ArrayList<byte[]> tags;
	private byte[] metric;
	static TSDB tsdb;
	
	public static void main(String[] args) throws IOException {

		 String sdate 	= "2000/01/01-00:00:00";
		 String edate 	= "2014/08/06-10:00:00";
		 String metric	= "r2time.stress2.test";
		 String zookeeper = "haisen24.ux.uis.no";
		 Query q = new TsdbQuery(tsdb);
		 Date d1 = new Date(sdate);
		 long timestamp = d1.getTime() / 1000; // To get time in millisecond
		 @SuppressWarnings("deprecation")
		 Date end_date = new Date(edate);
		 long end_timestamp = end_date.getTime() / 1000;// To get time in

		 q.setStartTime(timestamp); // Set Start time
		 q.setEndTime(end_timestamp); // set end time
		 
		 tsdb = new TSDB(new HBaseClient(zookeeper),Const.DATA_TABLE, Const.LOOKUP_TABLE);	
		 TsdbQuery ts = new TsdbQuery(tsdb);
		 //q.TsdbQuery(tsdb);
		 
		 Map<String, String> map = new HashMap<String, String>();
		 // map.put("host", "bikash|pc-0-227"); // tag key and tag value
		 map.put("host", "*");
//		 
		 q.toString();
		 
//		 setTimeSeries(metric, map, tsdb.aggregator(), false);
			 
		 
	}
	
	
	public class QueryClient{    
		private HBaseClient client;    
		private TSDB tsdb;        
		public QueryClient()    {    }        
		public void preparseTSDB()    
		{        
			client = new HBaseClient("localhost");     
			tsdb = new TSDB(client, "tsdb", "tsdb-uid");    
		}           
		private	void putTestData()    
		{                
			Map<String,String> tags = new HashMap<String, String>();                
			tsdb.addPoint("metricabc",123, 1, tags);    
		}
	}
	

}
