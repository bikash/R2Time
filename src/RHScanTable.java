/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 * 
 * Description: This class is used to get timeseries data from tsdb table.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.godhuli.rhipe.RHRaw;

public class RHScanTable  extends org.apache.hadoop.mapreduce.InputFormat<RHRaw, RHResult> 
    implements Configurable {

	private final static Log LOG = LogFactory.getLog(RHScanTable.class);

	public static boolean ValueIsString = false;
	public static boolean SingleCFQ = false;
	public static byte[][][] CFQ;
	/** Job parameter that specifies the input table. */
	public static final String INPUT_TABLE = "rhipe.hbase.tablename";

	/************SET BATCH for better performance****************/
	public static final String batch = "rhipe.hbase.set.batch";
		
	private Configuration conf = null;
	private HTable table = null;
	private Scan[] scans = null;
	private TableRecordReader trr = null;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader<RHRaw, RHResult> 
	    createRecordReader(InputSplit split, TaskAttemptContext context) 
	    throws IOException, InterruptedException {
		if (scans == null) {
			throw new IOException("No scans were provided");
		}
		if (table == null) {
			throw new IOException("No table was provided.");
		}
		if (trr == null) {
			trr = new TableRecordReader();
		}
		
		TableSplit tSplit = (TableSplit)split;
		LOG.info("Split in Record Reader  " + tSplit);
		Scan scan = new Scan(scans[0]);		
		scan.setStartRow(tSplit.getStartRow());
		scan.setStopRow(tSplit.getEndRow());
		LOG.info("Table in Record Reader  " + Bytes.toStringBinary(tSplit.getTableName()));
		trr.setScan(scan);
		trr.setHTable(table);
		trr.init();
		
		return trr;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		if (table == null) {
			throw new IOException("No table was provided.");
		}
		
		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			throw new IOException("Expecting at least one region.");
		}

		Set<InputSplit> splits = new HashSet<InputSplit>();
		for (int i = 0; i < keys.getFirst().length; i++) {
			String regionLocation = table.getRegionLocation(keys.getFirst()[i]).getServerAddress().getHostname();
			//LOG.info("Split length  " + keys.getFirst().length);
			for (Scan s : scans) {
				byte[] startRow = s.getStartRow();
				byte[] stopRow = s.getStopRow();
				
				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) && 
					 (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys.getFirst()[i]	: startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) 
										&& keys.getSecond()[i].length > 0 ? keys.getSecond()[i] : stopRow;					
								
					InputSplit split = new TableSplit(table.getTableName(), splitStart, splitStop, regionLocation);
					LOG.info("the current regionInfo's startKey is :"+Bytes.toStringBinary(splitStart)+"  , the current regionInfo's endkey is : "+Bytes.toStringBinary(splitStop) + "  , the current regionInfo's table is "+Bytes.toStringBinary(table.getTableName())+"  , the current regionInfo's regionLocation is :"+regionLocation );
					//LOG.info("Split server =>" + "  " + split);
					splits.add(split);
				}
			}
		}
		
		return new ArrayList<InputSplit>(splits);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.conf.Configurable#getConf()
	 */
	@Override
	public Configuration getConf() {
		return conf;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	
		RHHBaseRecorder.ValueIsString = conf.get("rhipe_hbase_values_are_string")!=null
		&& conf.get("rhipe_hbase_values_are_string").equals("TRUE");
		RHHBaseRecorder.SingleCFQ = conf.get("rhipe.hbase.single.cfq")!=null
		&& conf.get("rhipe.hbase.single.cfq").equals("TRUE");

		String tableName = conf.get(INPUT_TABLE);
		try {
		setHTable(new HTable(HBaseConfiguration.create(conf), tableName));
		} catch (Exception e) {
		LOG.error(StringUtils.stringifyException(e));
		}
		Scan[] scans = null;		
		scans = new Scan[] { new Scan() };
		LOG.info("Start Row Key" + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.rowlim.start"))));	
		LOG.info("End Row Key" + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.rowlim.end"))));
		LOG.info("Filter in  my " + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.filter"))));
		String[] x = conf.get("rhipe.hbase.mozilla.cacheblocks").split(":");
		LOG.info("cache " +  Integer.parseInt(x[0]) + " block " + Integer.parseInt(x[1]));
	    scans = Fun.generateScansTbl(  Integer.parseInt(x[0]),
	    Integer.parseInt(x[1]) == 1? true: false, 
		Integer.parseInt(conf.get("rhipe.hbase.set.batch"))); 	
		setScans(scans);
	}
	
	
	// get size of table. Not an efficient way of calculating. Calculation is done in client side.
	public static int getSizeofFile (int caching, boolean cacheBlocks, int batch) throws IOException   {
    	Scan s = new Scan();
    	s.setBatch(batch);
    	s.setCaching(caching); // 1 is the default in Scan, which will be bad for
        s.setCacheBlocks(false); // don't set to true for MR jobs

		@SuppressWarnings({ "deprecation", "static-access" })
		Configuration config = new HBaseConfiguration().create();
    	HTable tt = new HTable(config, "tsdb");
    	
    	ResultScanner ss = tt.getScanner(s);
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

    	LOG.info(" \n Size of HBase in kilo Bytes " + size);
		//return size;
		return size;
		
	}
	
	/**
	 * get column qualifier for whole table associate with row keys.
	 * 
	 * @param table
	 *            The table to get the data from.
	 */
	 public static Scan[] getAllColumnQualifier (HTable table) {
		 ArrayList<Scan> scans = new ArrayList<Scan>();
		 Scan scans2 = new Scan();
		 try{	          
	        	ResultScanner ss = table.getScanner(scans2);
	             for(Result r:ss){	            	
	                 for(KeyValue kv : r.raw()){ 
	                    scans2.addColumn(kv.getFamily(),kv.getQualifier());
	                 }	               
	             }
	             //return s;
	        } catch (IOException e){
	            e.printStackTrace();
	        }
		    scans.add(scans2);
			return scans.toArray(new Scan[scans.size()]);
	    }
	
	/**
	 * Allows subclasses to get the {@link HTable}.
	 */
	protected HTable getHTable() {
		return this.table;
	}

	/**
	 * Allows subclasses to set the {@link HTable}.
	 * 
	 * @param table
	 *            The table to get the data from.
	 */
	protected void setHTable(HTable table) {
		this.table = table;
	}
	
	/**
	 * @return
	 */
	public Scan[] getScans() {
		return scans;
	}

	/**
	 * @param scans The scans to use as boundaries.
	 */
	public void setScans(Scan[] scans) {
		this.scans = scans;
	}
	
	/**
	 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
	 * pairs.
	 */
	protected class TableRecordReader extends RecordReader<RHRaw, RHResult> {

		private ResultScanner scanner = null;
		private Scan scan = null;
		private HTable htable = null;
		private byte[] lastRow = null;
		private RHRaw key = null;
		private Result _value = null;
		private RHResult value = null;
		private Result oldresult = null;
		public void restart(byte[] firstRow) throws IOException {
			Scan newScan = new Scan(scan);
			newScan.setStartRow(firstRow);
			this.scanner = this.htable.getScanner(newScan);
		}

		public void init() throws IOException {
			restart(scan.getStartRow());
		}

		public void setHTable(HTable htable) {
			this.htable = htable;
		}

		public void setScan(Scan scan) {
			this.scan = scan;
		}
		public void close() {
			this.scanner.close();
		}

		public RHRaw getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public RHResult getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException,
				InterruptedException {
		}
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
			LOG.debug("recovered from  <-" );
			if (key == null)
				key = new RHRaw();
			if (value == null){
			    value = new RHResult();
			    oldresult = new Result();
			}
			try {
			    oldresult  = this.scanner.next();
			    if(oldresult != null) {
				value.set(oldresult); 
			    }
			} catch (IOException e) {
			    LOG.debug("recovered from " + StringUtils.stringifyException(e));
			    restart(lastRow);
			    scanner.next(); // skip presumed already mapped row
			    oldresult = scanner.next();
			    value.set(oldresult);
			}
			if (oldresult != null && oldresult.size() > 0) {
			    byte[] _b = oldresult.getRow();
			    key.set(_b);
			    lastRow = _b;
			    return true;
			}
			return false;
		}

		public float getProgress() {
			// Depends on the total number of tuples
			return 0;
		}
	}
}

