/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



/*
 * Description: Creating a custom Input Format, by help of RecordReader. Detail can be found in below links
 * https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-7/input-formats
 * http://hadoopi.wordpress.com/2013/05/27/understand-recordreader-inputsplit/
 * 
 * Implemented using RecordReader concepts.
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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.godhuli.rhipe.RHRaw;


public class RHHBaseRecorder  extends org.apache.hadoop.mapreduce.InputFormat<RHRaw, RHResult> 
    implements Configurable {

	private final static Log LOG = LogFactory.getLog(RHHBaseRecorder.class);
	public static boolean ValueIsString = false;
	public static boolean SingleCFQ = false;
	public static byte[][][] CFQ;
	/** Job parameter that specifies the input table. */
	public static final String INPUT_TABLE = "rhipe.hbase.tablename";
	
	/** Job parameter that specifies the filtering parameter for table. */
	public static final String Filter = "rhipe.hbase.filter";
	
	/************SET BATCH for better performance****************/
	public static final String batch = "rhipe.hbase.set.batch";
	
	/************SET BATCH for better performance****************/
	public static final String sizecal = "rhipe.hbase.set.size";
	
	/** Base-64 encoded array of scanners.	 */
	public static final String RHIPE_COLSPEC = "rhipe.hbase.colspec";
	
	private Configuration conf = null;
	private HTable table = null;
	private Scan[] scans = null;
	private TableRecordReader trr = null;
	
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 *	http://hadoop.apache.org/docs/current2/api/org/apache/hadoop/mapreduce/InputFormat.html
	 *
	 * RHRAW and RHRESULT is immutable raw bytes format used in RHIPE. For more details see RHIPE.
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
		//LOG.info("Table in Record Reader  " + Bytes.toStringBinary(tSplit.getTableName()));
		trr.setScan(scan);
		trr.setHTable(table);
		trr.init();
		return trr;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 * http://hadoop.apache.org/docs/current2/api/org/apache/hadoop/mapreduce/InputFormat.html
	 * 
	 * Get Number of splits between the start and end rowkeys.
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
										//LOG.info("Start Split now " +  Bytes.toStringBinary(splitStart));	
										//LOG.info("Start stop now " +   Bytes.toStringBinary(splitStop));
										//System.out.println("\n================" + Bytes.toLong(splitStart,0));
										//System.out.println("\n================" + splitStart.length);
									    //LOG.info("  Start row-***-   " +Bytes.toLong(startRow,0));
										//LOG.info("  End row-***-   " +Bytes.toLong(stopRow,0));								
					InputSplit split = new TableSplit(table.getTableName(), splitStart, splitStop, regionLocation);
					LOG.info("the current regionInfo's startKey is :"+Bytes.toStringBinary(splitStart)+"  , the current regionInfo's endkey is : "+Bytes.toStringBinary(splitStop) + "  , the current regionInfo's table is "+Bytes.toStringBinary(table.getTableName())+"  , the current regionInfo's regionLocation is :"+regionLocation );
					//LOG.info("Table Name  " + table.getTableName());
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
		if (conf.get(RHIPE_COLSPEC) != null) {
			try {
			    String[] cols = conf.get(RHIPE_COLSPEC).split(",");
			    ArrayList<Pair<String,String>> l = null;
			    if(cols.length > 0){
				l = new ArrayList<Pair<String,String>>(cols.length);
				for(int i=0;i < cols.length;i++) {
				    String[] x = cols[i].split(":");
				    if(x.length==1){
					l.add(new Pair<String,String>(x[0],null));
					LOG.info("Added family: "+x[0]);
				    } else{
					l.add(new Pair<String,String>(x[0],x[1]));
					LOG.info("Added "+x[0]+":"+x[1]);
				    }
				}
			    }
			    String[] x = conf.get("rhipe.hbase.mozilla.cacheblocks").split(":");
			    scans = Fun.generateScans(conf.get("rhipe.hbase.rowlim.start"),
						       conf.get("rhipe.hbase.rowlim.end"),
						       l,
						       Integer.parseInt(x[0]),
						       Integer.parseInt(x[1]) == 1? true: false);
			} catch (Exception e) {
				LOG.error("An error occurred.", e);
			}
		} else {
			//Scan[] scans = null;
			scans = new Scan[] { new Scan() };
		    LOG.info("Start Row Key" + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.rowlim.start"))));	
		    LOG.info("End Row Key" + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.rowlim.end"))));
		    //LOG.info("Filter in   " + Bytes.toStringBinary(org.apache.commons.codec.binary.Base64.decodeBase64(conf.get("rhipe.hbase.filter"))));
		    //LOG.info("Filter out  " + conf.get("rhipe.hbase.filter"));
			String[] x = conf.get("rhipe.hbase.mozilla.cacheblocks").split(":");
			LOG.info("cache " +  Integer.parseInt(x[0]) + " block " + Integer.parseInt(x[1]));
	    	scans = Fun.generateScansRows(conf.get("rhipe.hbase.rowlim.start"),
				       conf.get("rhipe.hbase.rowlim.end"),					     
				       Integer.parseInt(x[0]),
				       Integer.parseInt(x[1]) == 1? true: false, 
				       conf.get("rhipe.hbase.filter"),
				       Integer.parseInt(conf.get("rhipe.hbase.set.batch"))); 
	    	 //scans = getAllColumnQualifier(table);   			
		}
		setScans(scans);
	}

	public static int getSizeofFile (String st, String en, int caching, boolean cacheBlocks,  String filter, int batch) throws IOException  {
    	Scan s = new Scan();
    	s.setCacheBlocks(false); // don't set to true for MR jobs
    	LOG.info(" Calculation of File size in hbase at client side. ------   " );
     	if(st != null) {
     	    	byte[] stb1 = org.apache.commons.codec.binary.Base64.decodeBase64(st);
     	    	 //LOG.info("  Start row in ------   " +Bytes.toStringBinary(stb1));
     		s.setStartRow(stb1);
     	}
     	if(en != null) {
     	    byte[] enb2 = org.apache.commons.codec.binary.Base64.decodeBase64(en);
     	    //LOG.info("  End row in------   " +Bytes.toStringBinary(enb2));
     	    s.setStopRow(enb2);
     	}
     	//LOG.info("  Filter- -----   " +  filter);
     	RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
                 new RegexStringComparator( Bytes.toString(org.apache.commons.codec.binary.Base64.decodeBase64(filter))));     
     	s.setFilter(rowFilterRegex);
	    HTable tt  = new HTable(HBaseConfiguration.create(), "tsdb");		  	
    	ResultScanner ss = tt.getScanner(s);
    	int col = 0;
    	int size = 0;
    	for(Result r:ss){	
    		col = 0;
            for(KeyValue kv : r.raw()){ 
            	 col = col+kv.getLength(); 
            	 //System.out.print("\n Length keyValue " +kv.getLength() + "\n");    
            }	
            size = size + col/1000;
        }
    	System.out.print("\n Size " +size + "\n");    	
		return size;
	}
	
	
	
	 public static Scan[] getAllColumnQualifier (HTable table) {
		 ArrayList<Scan> scans = new ArrayList<Scan>();
		 Scan scans2 = new Scan();
		 try{	          
	        	int count =0;
	             
	             ResultScanner ss = table.getScanner(scans2);
	             for(Result r:ss){	            	
	                 for(KeyValue kv : r.raw()){ 
	                	//System.out.print("\n Rowkey " +org.apache.commons.codec.binary.Base64.encodeBase64String(kv.getRow())  ); 
	                   // System.out.print(new String(kv.getFamily()) + ":");
	                    //System.out.print(org.apache.commons.codec.binary.Base64.encodeBase64String(kv.getQualifier() )+ " ");	
	                    //s.addFamily(kv.getFamily());
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
	 * Get HBase Table
	 */
	protected HTable getHTable() {
		return this.table;
	}

	/**
	 * Allows subclasses to set the {@link HTable}.
	 * 
	 * @param table
	 *  The table to get the data from. Set Hbase table For our case it is normally tsdb.
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
			    //LOG.debug("recovered from " + StringUtils.stringifyException(e));
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
			return 0;
		}
	}
}
