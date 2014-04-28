

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.godhuli.rhipe.RHRaw;

/**
 * counts the number of userIDs
 * 
 * 
 */
public class MR {

    static class Mapper1 extends TableMapper<ImmutableBytesWritable, Result> {

        private int numRecords = 0;
        //private static final IntWritable one = new IntWritable(1);

        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            // extract userKey from the compositeKey (userId + counter)
            //ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
            try {
                context.write(row, values);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            numRecords++;
            //if ((numRecords % 10000) == 0) {
                context.setStatus("mapper processed " + numRecords + " records so far");
            //}
                
        }
    }

    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, RHResult, RHResult> {

        public void reduce(ImmutableBytesWritable key, Iterable<RHResult> values, Context context)
                throws IOException, InterruptedException {
           // int sum = 0;
           /* for (IntWritable val : values) {
                sum += val.get();
            }
			*/
           // Put put = new Put(key.get());
           //put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
           // System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
           // context.write(key, values);
        }
    }
    
   /* public static void main(String[] args) throws Exception {
        HBaseConfiguration conf = new HBaseConfiguration();
        Job job = new Job(conf, "Hbase_FreqCounter1");
        job.setJarByClass(MR.class);
        job.setOutputKeyClass(RHRaw.class);
        job.setOutputValueClass(RHResult.class);
        job.setInputFormatClass(RHHBaseRecorder.class);
        Scan scan = new Scan();
        String columns = "t:"; // comma seperated
        scan.addColumns(columns);
        scan.setFilter(new FirstKeyOnlyFilter());
        
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        
        TableMapReduceUtil.initTableMapperJob("tsdb", scan, Mapper1.class, ImmutableBytesWritable.class,
        		Result.class, job);
        //FileOutputFormat.setWorkOutputPath(job, new Path("/home/bikash/tmp/1111223"));
        TableMapReduceUtil.initTableReducerJob("tsdb", Reducer1.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/

}
