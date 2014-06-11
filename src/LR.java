import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.godhuli.rhipe.RHRaw;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;




public class LR {

    static class LinearRegressionMapper extends TableMapper<IntWritable, FloatWritable> {
    	private Path[] localFiles;
		FileInputStream fis = null;
		BufferedInputStream bis = null;
        private static final IntWritable KEY = new IntWritable(1);
        private static byte [] TS = new byte[4];
        private final FloatWritable VALUE = new FloatWritable(1);
        
        public void configure(Configuration job)
		{
			/**
			 * Read the distributed cache
			 */			
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        /**
		*  
		*  Linear-Regression costs function
		*  
		*  This will simply sum over the subset and calculate the predicted value y_predict(x) for the given features values and the current theta values
		*  Then it will subtract the true y values from the y_predict(x) value for every input record in the subset
		*  
		*  J(theta) = sum((y_predict(x)-y)^2)
		*  y_predict(x) = theta(0)*x(0) + .... + theta(i)*x(i)
		* 
		*/
        System.arraycopy(row.get(), 3, TS, 0, 4); 
        for (KeyValue kv : values.raw()) {
        	final short delta = (short) (( org.apache.hadoop.hbase.util.Bytes.toShort(kv.getQualifier()) & 0xFFFF) >>> 4);
            int timestamp = org.apache.hadoop.hbase.util.Bytes.toInt(TS)+ delta;	
           	//System.out.print("\n Base Timestamp as Rowkey => "+ org.apache.hadoop.hbase.util.Bytes.toInt(TS) + " -- Timestamp " + timestamp +  "\n");   
           	// System.out.print("Value " + org.apache.hadoop.hbase.util.Bytes.toFloat(kv.getValue() )+ "\n");   
             	/**
    			 * read the values and convert them to floats
    			 */
             	List<Float> val = new ArrayList<Float>();
             	val.add(org.apache.hadoop.hbase.util.Bytes.toFloat(kv.getValue()));
           	    //VALUE.set(org.apache.hadoop.hbase.util.Bytes.toFloat(kv.getValue()));
             	System.out.print("Value " + org.apache.hadoop.hbase.util.Bytes.toFloat(kv.getValue() )+ "\n");  
             	/**
    			 * calculate the costs
    			 * 
    			 */
    			
             	//context.write(KEY, new FloatWritable(costs(val)));
           	    //KEY.set(timestamp);
           	    try {
           	    	//context.write(KEY,VALUE); //Define key as 1 same for every values
           	    	context.write(KEY, new FloatWritable(costs(val)));
           	    }
           	    catch (InterruptedException e) {
           	    	throw new IOException(e);
           	    }
           	    //numRecords++;
             }
        }
        
        
		private final float costs(List<Float> values)
		{
			/**
			* Load the cache files
			*/

			File file = new File("/home/bikash/repos/r2time/examples/theta.csv");

			float costs = 0;
			
			try {
				FileInputStream fis = new FileInputStream(file);
				BufferedInputStream bis = new BufferedInputStream(fis);
				
				BufferedReader d = new BufferedReader(new InputStreamReader(bis));
				String line = d.readLine();
				
				//all right we have all the theta values, lets convert them to floats
				String[] theta = line.split(",");
				
				//first value is the y value
				float y = values.get(0);
				
				/**
				 * Calculate the costs for each record in values
				 */
				for(int j = 0; j < values.size(); j++)
				{
						//bias calculation
						if(j == 0)
							costs += (new Float(theta[j]))*1;
						else
							costs += (new Float(theta[j]))*values.get(j);

				}
				
				// Subtract y and square the costs
				costs = (costs -y)*(costs - y);
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return costs;		
		}
    }

		

	
	public static class LinearRegressionReducer extends TableReducer <IntWritable, FloatWritable, ImmutableBytesWritable>
	{
		public static final byte[] CF = "cf".getBytes();
    	public static final byte[] COUNT = "count".getBytes();
    	@Override
		public void reduce(IntWritable key, Iterable<FloatWritable> value, Context context) 
				throws IOException, InterruptedException {
			/**
			 * The reducer just has to sum all the values for a given key
			 * 
			 */
			float sum = 0;
		    int count =0;
		    for (FloatWritable val : value) {
		       sum += val.get();
		       count++;
		    }
		    Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(key.get()));
			System.out.print("Sum value => " + sum+ "\n");  
			put.add(org.apache.hadoop.hbase.util.Bytes.toBytes("number"), org.apache.hadoop.hbase.util.Bytes.toBytes(""), org.apache.hadoop.hbase.util.Bytes.toBytes(sum));
			context.write(null, put);
			//output.collect(key, new FloatWritable(sum));
			
		}
		
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		   
	    Configuration conf = HBaseConfiguration.create();
	    String zookeeperQuorum = "haisen24.ux.uis.no";
	    String HBaseMaster = "haisen23.ux.uis.no:60000";
//	    String zookeeperQuorum = "localhost";
//	    String HBaseMaster = "localhost:60000";
	    conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
//	    conf.set("mapred.job.tracker", "haisen22.ux.uis.no:8021");
//	    conf.set("fs.default.name", "hdfs://haisen20.ux.uis.no:8020");

	    conf.set("hbase.master", HBaseMaster);
        Job job = new Job(conf, "Linear Regression");
        job.setJarByClass(LR.class);
        
        /**
		* Try to load the theta values into the distributed cache
		*/
		try {
		  //make sure this is your path to the cache file in the hadoop file system
			DistributedCache.addCacheFile(new URI("/home/bikash/repos/r2time/examples/theta.csv"), conf);
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
        /***create scan object ***/
        DataType dt = new DataType();
    	dt.setHbaseClient(zookeeperQuorum);
    	String[] tagk = {"1","host"};
        String[] tagv = {"1","*"};
        
    	String[] val =  DataType.getRowkeyFilter("1980/01/01-00:00:00","2014/02/22-10:00:00", "r2time.stress.test",  tagk, tagv);
    	Scan scans = new Scan();
    	scans.setStartRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[0]));
        scans.setStopRow(org.apache.commons.codec.binary.Base64.decodeBase64(val[1]));    
        RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
               new RegexStringComparator( org.apache.hadoop.hbase.util.Bytes.toString(org.apache.commons.codec.binary.Base64.decodeBase64(val[2]))));
        scans.setFilter(rowFilterRegex); 
        scans.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scans.setCacheBlocks(false);
        
        
        //job.setOutputKeyClass(RHRaw.class);
        //job.setOutputValueClass(RHResult.class);
        job.setInputFormatClass(RHHBaseRecorder.class);
        String columns = "t:"; // comma seperated
        scans.addColumns(columns);

        
        //job.setMapperClass(Mapper1.class);
        TableMapReduceUtil.initTableMapperJob("tsdb", scans, LinearRegressionMapper.class, IntWritable.class,FloatWritable.class, job);
        job.setReducerClass(LinearRegressionReducer.class);
        //job.setNumReduceTasks(1);    // at least one, adjust as required
        TableMapReduceUtil.initTableReducerJob("out", LinearRegressionReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}