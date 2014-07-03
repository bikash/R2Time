

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.godhuli.rhipe.RHRaw;
/**
 * Average of data points in OpenTSDB
 * 
 * 
*/
public class MR {

    static class Mapper1 extends TableMapper<IntWritable, FloatWritable> {

        //private int numRecords = 0;
        private static final IntWritable KEY = new IntWritable(1);
        private static byte [] TS = new byte[4];
        private final FloatWritable VALUE = new FloatWritable(1);
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        	
        	 System.arraycopy(row.get(), 3, TS, 0, 4);  // get timestamp from row key
             for (KeyValue kv : values.raw()) {
             	//final short delta = (short) (( org.apache.hadoop.hbase.util.Bytes.toShort(kv.getQualifier()) & 0xFFFF) >>> 4);
             	//int timestamp = org.apache.hadoop.hbase.util.Bytes.toInt(TS)+ delta;	
           	    //System.out.print("\nBase Timestamp as Rowkey => "+ org.apache.hadoop.hbase.util.Bytes.toInt(TS) + " -- Timestamp " + timestamp +  "\n");   
           	    //System.out.print("Value " + org.apache.hadoop.hbase.util.Bytes.toLong(kv.getValue() )+ "\n");   
           	    VALUE.set(org.apache.hadoop.hbase.util.Bytes.toLong(kv.getValue()));
           	    //KEY.set(timestamp);
           	    try {
           	    	context.write(KEY,VALUE); //Define key as 1 same for every values
           	    }
           	    catch (InterruptedException e) {
           	    	throw new IOException(e);
           	    }
           	    //numRecords++;
             }
        }
    }

    
    public static class MyReducer extends  TableReducer<IntWritable, FloatWritable, ImmutableBytesWritable> {
    	public static final byte[] CF = "cf".getBytes();
    	public static final byte[] COUNT = "count".getBytes();
    	@Override
		 public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) 
				 throws IOException, InterruptedException {
		    //int sum = 0;
		    int count =0;
		    for (FloatWritable value : values) {
		       //sum += value.get();
		       count++;
		    }
		    //float avg = sum/count;
		    Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(key.get()));
	
		   // put.add org.apache.hadoop.hbase.util.Bytes.toBytes(sum));
		    //System.out.print("Mean value => " + avg+ "\n");  
		    System.out.print("Total data point => " + count+ "\n");
		    put.add(org.apache.hadoop.hbase.util.Bytes.toBytes("number"), org.apache.hadoop.hbase.util.Bytes.toBytes(""), org.apache.hadoop.hbase.util.Bytes.toBytes(count));
		    context.write(null, put);
		 }
    }
    
   @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
//	    Configuration config = new Configuration();
//	    config.set("fs.default.name", "hdfs://home/bikash/tmp");
//	    config.set("mapred.job.tracker", "localhost:50030/");
// "1973/01/01-00:00:00" "2014/07/22-10:00:00" "r2time.load.test1" "haisen24.ux.uis.no"
	   String sdate 	= "1973/01/01-00:00:00";
	   String edate 	= "1975/01/01-01:00:00";
	   String metric	= "r2time.load.test1";
	   String zookeeper = "haisen24.ux.uis.no";
	   if (args.length > 0) {
		   sdate 		= args[0];
		   edate 		= args[1];
		   metric 		= args[2];
		   zookeeper 	= args[3];
	   }
	   else
	   {
		   System.err.println("Please enter the start data, end data, metric and zookeeper.");
	       System.exit(1); 
	   }
	    
	    Configuration conf = HBaseConfiguration.create();
	    String zookeeperQuorum = zookeeper;
	    String HBaseMaster = "haisen23.ux.uis.no:60000";
//	    String zookeeperQuorum = "localhost";
//	    String HBaseMaster = "localhost:60000";
	    conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
//	    conf.set("mapred.job.tracker", "haisen22.ux.uis.no:8021");
//	    conf.set("fs.default.name", "hdfs://haisen20.ux.uis.no:8020");

	    conf.set("hbase.master", HBaseMaster);
        Job job = new Job(conf, "MapReduce by Hbase");
        job.setJarByClass(MR.class);
        
        /***create scan object ***/
        DataType dt = new DataType();
    	dt.setHbaseClient(zookeeperQuorum);
    	String[] tagk = {"1","host"};
        String[] tagv = {"1","*"};
        
    	String[] val =  DataType.getRowkeyFilter(sdate,edate,metric,tagk, tagv);
    	Scan scans = new Scan();
    	scans.setStartRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[0]));
        scans.setStopRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[1]));    
        RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
               new RegexStringComparator( org.apache.hadoop.hbase.util.Bytes.toString(org.apache.commons.codec.binary.Base64.decodeBase64(val[2]))));
        scans.setFilter(rowFilterRegex); 
        scans.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scans.setCacheBlocks(false);
        
        
        job.setOutputKeyClass(RHRaw.class);
        job.setOutputValueClass(RHResult.class);
        job.setInputFormatClass(RHHBaseRecorder.class);
        String columns = "t:"; // comma seperated
        scans.addColumns(columns);

        
        //job.setMapperClass(Mapper1.class);
        TableMapReduceUtil.initTableMapperJob("tsdb", scans, Mapper1.class, IntWritable.class,FloatWritable.class, job);
        job.setReducerClass(MyReducer.class);
        //job.setNumReduceTasks(1);    // at least one, adjust as required
        
        //FileOutputFormat.setWorkOutputPath(job, new Path("/home/bikash/tmp/out"));  // adjust directories as required
        //FileOutputFormat.setOutputPath(job, new Path("/home/bikash/tmp/1111223"));
        TableMapReduceUtil.initTableReducerJob("out", MyReducer.class, job);

        //FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile"));  // adjust directories as required

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
