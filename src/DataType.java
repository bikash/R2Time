/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 * 
 * Description: This class is used to get timeseries data from tsdb table.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.uid.NoSuchUniqueName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseClient;

public class DataType {

	private final static Log LOG = LogFactory.getLog(DataType.class);
	private static byte[] metrics;
	public static byte[] valuetagk;
	public static byte[] valuetagv;
	
	/** Job parameter that specifies the input table. */
	private String HBASE_CLIENT = "";
	
	private Configuration conf = null;
	/** The TSDB we belong to. */
	// private final static TSDB tsdb;

	/** Value used for timestamps that are uninitialized. */
	private static final int UNSET = -1;

	/** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
	private static int start_time = UNSET;

	/** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
	private static int end_time = UNSET;
	private static int sample_interval = 1;
	/**
	 * Tags of the metrics being looked up. Each tag is a byte array holding the
	 * ID of both the name and value of the tag. Invariant: an element cannot be
	 * both in this array and in group_bys.
	 */
	private ArrayList<byte[]> tags;
	/**
	 * Tags by which we must group the results. Each element is a tag ID.
	 * Invariant: an element cannot be both in this array and in {@code tags}.
	 */
	private ArrayList<byte[]> group_bys;

	/**
	 * Values we may be grouping on. For certain elements in {@code group_bys},
	 * we may have a specific list of values IDs we're looking for. Those IDs
	 * are stored in this map. The key is an element of {@code group_bys} (so a
	 * tag name ID) and the values are tag value IDs (at least two).
	 */
	private ByteMap<byte[][]> group_by_values;
	private byte[] metric;

	/**
	 * Charset to use with our server-side row-filter. We use this one because
	 * it preserves every possible byte unchanged.
	 */
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	private REXP anull;
	private HBaseAdmin _admin;
    private static TSDB tsdb;
    
    /* Constructor for initilization of tsdb */
	public DataType() throws MasterNotRunningException,
			ZooKeeperConnectionException {
//		tsdb = new TSDB(new HBaseClient(HBASE_CLIENT),
//				Const.DATA_TABLE, Const.LOOKUP_TABLE);
		REXP.Builder returnvalue = REXP.newBuilder();
		returnvalue.setRclass(REXP.RClass.NULLTYPE);
		anull = returnvalue.build();
		REXP.Builder keycontainer = REXP.newBuilder();
		keycontainer.setRclass(REXP.RClass.RAW);

	}

	public void setHbaseClient(String host) {
		this.HBASE_CLIENT = host;
		tsdb = new TSDB(new HBaseClient(HBASE_CLIENT),
				Const.DATA_TABLE, Const.LOOKUP_TABLE);	
	}
	
	public void setConf(Configuration conf) {
		this.conf = conf;
		//Const.HBASE_CLIENT = conf.get("r2time.hbase.client");
		
	}
	public static long getLong(final byte[] b, final int offset) {
		if (b.length < 3) {
			return (b[offset + 0] & 0xFFL) << 56
					| (b[offset + 1] & 0xFFL) << 48;

		}
		if (b.length < 2) {
			return (b[offset + 0] & 0xFFL) << 56;
		}
		if (b.length < 4) {
			return (b[offset + 0] & 0xFFL) << 56
					| (b[offset + 1] & 0xFFL) << 48
					| (b[offset + 2] & 0xFFL) << 40;
		}
		if (b.length < 5) {
			return (b[offset + 0] & 0xFFL) << 56
					| (b[offset + 1] & 0xFFL) << 48
					| (b[offset + 2] & 0xFFL) << 40
					| (b[offset + 3] & 0xFFL) << 32;
		}
		if (b.length < 6) {
			return (b[offset + 0] & 0xFFL) << 56
					| (b[offset + 1] & 0xFFL) << 48
					| (b[offset + 2] & 0xFFL) << 40
					| (b[offset + 3] & 0xFFL) << 32
					| (b[offset + 4] & 0xFFL) << 24;
		} else {
			return (b[offset + 0] & 0xFFL) << 56
					| (b[offset + 1] & 0xFFL) << 48
					| (b[offset + 2] & 0xFFL) << 40
					| (b[offset + 3] & 0xFFL) << 32
					| (b[offset + 4] & 0xFFL) << 24
					| (b[offset + 5] & 0xFFL) << 16
					| (b[offset + 6] & 0xFFL) << 8
					| (b[offset + 7] & 0xFFL) << 0;
		}
	}

	public static void setInt(final byte[] b, final int n) {
		setInt(b, n, 0);
	}

	/**
	 * Writes a big-endian 4-byte int at an offset in the given array.
	 * 
	 * @param b
	 *            The array to write to.
	 * @param offset
	 *            The offset in the array to start writing at.
	 * @throws IndexOutOfBoundsException
	 *             if the byte array is too small.
	 */
	public static void setInt(final byte[] b, final int n, final int offset) {
		b[offset + 0] = (byte) (n >>> 24);
		b[offset + 1] = (byte) (n >>> 16);
		b[offset + 2] = (byte) (n >>> 8);
		b[offset + 3] = (byte) (n >>> 0);
	}

	/**
	 * Copies the specified byte array at the specified offset in the row key.
	 * 
	 * @param row
	 *            The row key into which to copy the bytes.
	 * @param offset
	 *            The offset in the row key to start writing at.
	 * @param bytes
	 *            The bytes to copy.
	 */
	private static void copyInRowKey(final byte[] row, final short offset,
			final byte[] bytes) {
		System.arraycopy(bytes, 0, row, offset, bytes.length);
	}

	private static Object resizeArray(Object oldArray, int newSize) {

		int oldSize = java.lang.reflect.Array.getLength(oldArray);

		Class elementType = oldArray.getClass().getComponentType();
		Object newArray = java.lang.reflect.Array.newInstance(elementType,
				newSize);
		int preserveLength = Math.min(oldSize, newSize);
		// System.out.println("\n================" +newSize);
		if (preserveLength > 0)
			System.arraycopy(oldArray, 0, newArray, 0, preserveLength);
		return newArray;

	}

	public static byte[] long2byte(long l) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.SIZE / 8);
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeLong(l);
		byte[] result = baos.toByteArray();
		dos.close();
		return result;
	}

	/**
	 * Writes a big-endian 8-byte long at an offset in the given array.
	 * 
	 * @param b
	 *            The array to write to.
	 * @param offset
	 *            The offset in the array to start writing at.
	 * @throws IndexOutOfBoundsException
	 *             if the byte array is too small.
	 */
	public static void setLong(final byte[] b, final long n, final int offset) {
		b[offset + 0] = (byte) (n >>> 56);
		b[offset + 1] = (byte) (n >>> 48);
		b[offset + 2] = (byte) (n >>> 40);
		b[offset + 3] = (byte) (n >>> 32);
		b[offset + 4] = (byte) (n >>> 24);
		b[offset + 5] = (byte) (n >>> 16);
		b[offset + 6] = (byte) (n >>> 8);
		b[offset + 7] = (byte) (n >>> 0);
	}

	/**
	 * Creates a new byte array containing a big-endian 8-byte long integer.
	 * 
	 * @param n
	 *            A long integer.
	 * @return A new byte array containing the given value.
	 */
	public static byte[] fromLong(final long n) {
		final byte[] b = new byte[8];
		setLong(b, n, 0);
		return b;
	}

	public static long byte2long(byte[] b) throws IOException {
		ByteArrayInputStream baos = new ByteArrayInputStream(b);
		DataInputStream dos = new DataInputStream(baos);
		long result = dos.readLong();
		dos.close();
		return result;
	}

	public static byte[] getTagK(HTable table) {

		byte[] familyk1 = Bytes.toBytes("id");
		byte[] qualifierk1 = Bytes.toBytes("tagk");
		byte[] tagk = new byte[13];
		ByteBuffer bbf1 = ByteBuffer.allocate(10);
		try {
			ResultScanner scanner1 = table.getScanner(familyk1, qualifierk1);
			for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
				tagk = rr.getRow();
				bbf1.put(tagk);
				// System.out.println("Found Tag key: " + tagk);
			}

		} catch (java.io.IOException exp) {
			exp.printStackTrace();
		}
		tagk = bbf1.array();
		return tagk;
	}

	public static byte[] getTagv(HTable table) {

		byte[] familyv1 = Bytes.toBytes("id");
		byte[] qualifierv1 = Bytes.toBytes("tagv");
		byte[] tagv = new byte[13];
		ByteBuffer bbf2 = ByteBuffer.allocate(10);
		try {
			ResultScanner scanner2 = table.getScanner(familyv1, qualifierv1);
			for (Result rr = scanner2.next(); rr != null; rr = scanner2.next()) {
				tagv = rr.getRow();
				bbf2.put(tagv);
				// System.out.println("Found Tag key: " + tagv);
			}
		} catch (java.io.IOException exp) {
			exp.printStackTrace();
		}

		tagv = bbf2.array();
		return tagv;
	}

	public static byte[] getMetrics(HTable table) {

		byte[] familym = Bytes.toBytes("id");
		byte[] qualifierm = Bytes.toBytes("metrics");
		byte[] mertics1 = new byte[13];
		ByteBuffer bbf3 = ByteBuffer.allocate(10);
		try {
			ResultScanner scanner3 = table.getScanner(familym, qualifierm);
			for (Result rr = scanner3.next(); rr != null; rr = scanner3.next()) {
				mertics1 = rr.getRow();
				bbf3.put(mertics1);
				// System.out.println("Found Tag key: " + tagv);
			}
		} catch (java.io.IOException exp) {
			exp.printStackTrace();
		}
		mertics1 = bbf3.array();
		return mertics1;
	}

	public static byte[] getId(String key) throws IOException {

		@SuppressWarnings("deprecation")
		HBaseConfiguration config = new HBaseConfiguration();
		HTable table = new HTable(config, "tsdb-uid");

		Get gk = new Get(Bytes.toBytes(key));
		Result rk = table.get(gk);
		byte[] familyk = Bytes.toBytes("id");
		byte[] qualifierk = Bytes.toBytes("tagk");
		if (rk != null) {
			valuetagk = rk.getValue(familyk, qualifierk);
		}

		return valuetagk;

	}

	/**
	 * Helper comparison function to compare tag name IDs.
	 * 
	 * @param name_width
	 *            Number of bytes used by a tag name ID.
	 * @param tag
	 *            A tag (array containing a tag name ID and a tag value ID).
	 * @param group_by
	 *            A tag name ID.
	 * @return {@code true} number if {@code tag} should be used next (because
	 *         it contains a smaller ID), {@code false} otherwise.
	 */
	private boolean isTagNext(final short name_width, final byte[] tag,
			final byte[] group_by) {
		if (tag == null) {
			return false;
		} else if (group_by == null) {
			return true;
		}
		final int cmp = org.hbase.async.Bytes.memcmp(tag, group_by, 0,
				name_width);
		if (cmp == 0) {
			throw new AssertionError("invariant violation: tag ID "
					+ Arrays.toString(group_by) + " is both in 'tags' and"
					+ " 'group_bys' in " + this);
		}
		return cmp < 0;
	}

	/**
	 * Appends the given ID to the given buffer, followed by "\\E".
	 */
	private static void addId(final StringBuilder buf, final byte[] id) {
		boolean backslash = false;
		for (final byte b : id) {
			buf.append((char) (b & 0xFF));
			if (b == 'E' && backslash) { // If we saw a `\' and now we have a
											// `E'.
				// So we just terminated the quoted section because we just
				// added \E
				// to `buf'. So let's put a litteral \E now and start quoting
				// again.
				buf.append("\\\\E\\Q");
			} else {
				backslash = b == '\\';
			}
		}
		buf.append("\\E");
	}

	static ArrayList<byte[]> resolveAll(final TSDB tsdb,
			final Map<String, String> tags) throws NoSuchUniqueName {
		return resolveAllInternal(tsdb, tags, false);
	}

	private static ArrayList<byte[]> resolveAllInternal(final TSDB tsdb,
			final Map<String, String> tags, final boolean create)
			throws NoSuchUniqueName {
		final ArrayList<byte[]> tag_ids = new ArrayList<byte[]>(tags.size());
		for (final Map.Entry<String, String> entry : tags.entrySet()) {
			final byte[] tag_id = (tsdb.tag_names.getId(entry.getKey()));
			final byte[] value_id = (tsdb.tag_values.getId(entry.getValue()));
			final byte[] thistag = new byte[tag_id.length + value_id.length];
			System.arraycopy(tag_id, 0, thistag, 0, tag_id.length);
			System.arraycopy(value_id, 0, thistag, tag_id.length,
					value_id.length);
			tag_ids.add(thistag);
		}
		// Now sort the tags.
		Collections.sort(tag_ids, org.hbase.async.Bytes.MEMCMP);
		return tag_ids;
	}

	/**
	 * Sets the server-side regexp filter on the scanner. In order to find the
	 * rows with the relevant tags, we use a server-side filter that matches a
	 * regular expression on the row key.
	 */
	public StringBuilder creatFilter(final TSDB tsdb) throws IOException {
		if (group_bys != null) {
			Collections.sort(group_bys, Bytes.BYTES_COMPARATOR);
		}
		final short name_width = 3;
		final short value_width = 3;
		final short tagsize = (short) (name_width + value_width);

		// Generate a regexp for our tags. Say we have 2 tags: { 0 0 1 0 0 2 }
		// and { 4 5 6 9 8 7 }, the regexp will be:
		// "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
		final StringBuilder buf = new StringBuilder(15 // "^.{N}" + "(?:.{M})*"
														// + "$"
				+ ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
				* (3 + (group_bys == null ? 0 : group_bys.size() * 3))));
		// In order to avoid re-allocations, reserve a bit more w/ groups ^^^

		// Alright, let's build this regexp. From the beginning...
		buf.append("(?s)" // Ensure we use the DOTALL flag.
				+ "^.{")
		// ... start by skipping the metric ID and timestamp.
				.append(3 + 4).append("}");

		final Iterator<byte[]> tags = this.tags.iterator();
		final Iterator<byte[]> group_bys = (this.group_bys == null ? new ArrayList<byte[]>(
				0).iterator() : this.group_bys.iterator());
		byte[] tag = tags.hasNext() ? tags.next() : null;
		byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
		// Tags and group_bys are already sorted. We need to put them in the
		// regexp in order by ID, which means we just merge two sorted lists.
		do {
			// Skip any number of tags.
			buf.append("(?:.{").append(tagsize).append("})*\\Q");
			if (isTagNext(name_width, tag, group_by)) {
				addId(buf, tag);
				tag = tags.hasNext() ? tags.next() : null;
			} else { // Add a group_by.

				addId(buf, group_by);
				final byte[][] value_ids = (group_by_values == null ? null
						: group_by_values.get(group_by));
				if (value_ids == null) { // We don't want any specific ID...
					buf.append(".{").append(value_width).append('}'); // Any
																		// value
																		// ID.
				} else { // We want specific IDs. List them: /(AAA|BBB|CCC|..)/
					buf.append("(?:");
					for (final byte[] value_id : value_ids) {
						buf.append("\\Q");
						addId(buf, value_id);
						buf.append('|');
					}
					// Replace the pipe of the last iteration.
					buf.setCharAt(buf.length() - 1, ')');

				}
				group_by = group_bys.hasNext() ? group_bys.next() : null;
			}
		} while (tag != group_by); // Stop when they both become null.
		// Skip any number of tags before the end.
		buf.append("(?:.{").append(tagsize).append("})*$");
		//String filter1 = Bytes.toStringBinary(Bytes.toBytes(buf.toString()));
		return buf;

	}

	public void setTimeSeries(final String metric,
			final Map<String, String> tags, final TSDB tsdb) throws IOException {
		findGroupBys(tags, tsdb);
		this.metric = tsdb.metrics.getId(metric);
		this.tags = Tags.resolveAll(tsdb, tags);

	}

	/**
	 * Extracts all the tags we must use to group results.
	 * <ul>
	 * <li>If a tag has the form {@code name=*} then we'll create one group per
	 * value we find for that tag.</li>
	 * <li>If a tag has the form {@code name= v1,v2,..,vN}} then we'll create
	 * {@code N} groups.</li>
	 * </ul>
	 * In the both cases above, {@code name} will be stored in the
	 * {@code group_bys} attribute. In the second case specifically, the
	 * {@code N} values would be stored in {@code group_by_values}, the key in
	 * this map being {@code name}.
	 * 
	 * @param tags
	 *            The tags from which to extract the 'GROUP BY's. Each tag that
	 *            represents a 'GROUP BY' will be removed from the map passed in
	 *            argument.
	 * @throws IOException
	 */
	public void findGroupBys(final Map<String, String> tags, final TSDB tsdb)
			throws IOException {
		final Iterator<Map.Entry<String, String>> i = tags.entrySet()
				.iterator();
		while (i.hasNext()) {
			final Map.Entry<String, String> tag = i.next();
			final String tagvalue = tag.getValue();

			if (tagvalue.equals("*") // 'GROUP BY' with any value.
					|| tagvalue.indexOf('|', 1) >= 0) { // Multiple possible
														// values.
				// System.out.println("Scanning table... " + tag.getKey());
				if (group_bys == null) {
					group_bys = new ArrayList<byte[]>();
				}
				group_bys.add(tsdb.tag_names.getId(tag.getKey()));
				// i.remove();
				if (tagvalue.charAt(0) == '*') {
					i.remove();
					continue; // For a 'GROUP BY' with any value, we're done.
				}
				// 'GROUP BY' with specific values. Need to split the values
				// to group on and store their IDs in group_by_values.

				final String[] values = Tags.splitString(tagvalue, '|');
				// for( int k=0; k<values.length; k++)
				// {
				// System.out.println("Scanning table1... " + values[k]);
				// }
				if (group_by_values == null) {
					group_by_values = new ByteMap<byte[][]>();
				}

				final short value_width = tsdb.tag_values.width();

				final byte[][] value_ids = new byte[values.length][value_width];
				// for( int k=0; k<values.length; k++)
				// {

				group_by_values.put(tsdb.tag_names.getId(tag.getKey()),
						value_ids);
				// }
				for (int j = 0; j < values.length; j++) {
					// System.out.println("value width... " +values[j]);
					final byte[] value_id = tsdb.tag_values.getId(values[j]);
					System.arraycopy(value_id, 0, value_ids[j], 0, value_width);
				}
				i.remove();
			}
		}
	}

	public static void setStartTime(long timestamp) {
		start_time = (int) timestamp;
	}

	public static void setEndTime(long timestamp) {
		end_time = (int) timestamp;
		// return (end_time);
	}

	public static long getEndTime() {
		if (end_time == UNSET) {
			setEndTime(System.currentTimeMillis() / 1000);
		}
		return end_time;
	}

	public static long getStartTime() {
		if (start_time == UNSET) {
			throw new IllegalStateException("setStartTime was never called!");
		}
		return start_time & 0x00000000FFFFFFFFL;
	}

	/** Returns the UNIX timestamp from which we must start scanning. */
	static long getScanStartTime() {
		// The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
		// the following. Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
		// start_time = ... 12:31:00. If we initialize the scanner to look
		// only 10 minutes before, we'll start scanning at time=12:21, which
		// will
		// give us the row that starts at 12:30 (remember: rows are always
		// aligned
		// on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
		// But we need to start scanning at least 1 row before, so we actually
		// look back by twice MAX_TIMESPAN. Only when start_time is aligned on a
		// MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra
		// row,
		// but this doesn't really matter.
		// Additionally, in case our sample_interval is large, we need to look
		// even further before/after, so use that too.
		
		final long ts = getStartTime() - Const.MAX_TIMESPAN * 2
				- sample_interval;
		//System.out.println(ts + " "+ Const.MAX_TIMESPAN * 2 + " == "+getStartTime() );
		return ts > 0 ? ts : 0;
	}

	/** Returns the UNIX timestamp at which we must stop scanning. */
	static long getScanEndTime() {
		// For the end_time, we have a different problem. For instance if our
		// end_time = ... 12:30:00, we'll stop scanning when we get to 12:40,
		// but
		// once again we wanna try to look ahead one more row, so to avoid this
		// problem we always add 1 second to the end_time. Only when the
		// end_time
		// is of the form HH:59:59 then we will scan ahead an extra row, but
		// once
		// again that doesn't really matter.
		// Additionally, in case our sample_interval is large, we need to look
		// even further before/after, so use that too.
		return getEndTime() + Const.MAX_TIMESPAN + 1 + sample_interval;
	}

	public static void test(String[] args) throws IOException {
		// getTimeSereiesData("","","","", "");
		String sdate = "2013/03/12";
		String edate = "2013/03/14";
		String tagkey = "host";
		String tagvalue = "pc-0-227";
		String metricsval = "proc.loadavg.1m";

		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these
		// can
		// be found on the CLASSPATH
		@SuppressWarnings("deprecation")
		HBaseConfiguration config = new HBaseConfiguration();

		// This instantiates an HTable object that connects you to
		// the "myLittleHBaseTable" table.
		HTable table = new HTable(config, "tsdb-uid");

		// Now, to retrieve the data we just wrote. The values that come back
		// are
		// Result instances. Generally, a Result is an object that will package
		// up
		// the hbase return into the form you find most palatable.

		Get g = new Get(Bytes.toBytes(metricsval));
		Result r = table.get(g);
		byte[] value = r
				.getValue(Bytes.toBytes("id"), Bytes.toBytes("metrics"));
		// If we convert the value bytes, we should get back 'Some Value', the
		// / value we inserted at this location.
		String valueStr = Bytes.toString(value);

		byte[] family = Bytes.toBytes("id");
		byte[] qualifier = Bytes.toBytes("metrics");

		if (r != null) {
			metrics = r.getValue(family, qualifier);
			// String valueStr1 = Bytes.toString(value1);
			// System.out.println("\n=========metrics ID=======" +metrics);
			// System.out.println("\n================" +getLong(metrics,0));
			// System.out.println("\n================");
		}

		// Tag K
		Get gk = new Get(Bytes.toBytes(tagkey));
		Result rk = table.get(gk);
		byte[] familyk = Bytes.toBytes("id");
		byte[] qualifierk = Bytes.toBytes("tagk");
		if (rk != null) {
			valuetagk = rk.getValue(familyk, qualifierk);
			// System.out.println("\n=========Tag Key=======" +valuetagk);
			// System.out.println("\n================" +getLong(valuetagk,0));
			// System.out.println("\n================");
		}

		// Tag v
		Get gv = new Get(Bytes.toBytes(tagvalue));
		Result rv = table.get(gv);
		byte[] familyv = Bytes.toBytes("id");
		byte[] qualifierv = Bytes.toBytes("tagv");
		if (rv != null) {
			valuetagv = rv.getValue(familyv, qualifierv);
			// System.out.println("\n=====Tag Value===========" +valuetagv);
			// System.out.println("\n================" +getLong(valuetagv,0));
			// System.out.println("\n================");
		}

		short pos = 0;

		final byte[] row = new byte[13];
		copyInRowKey(row, pos, metrics);
		pos += 4;
		// copyInRowKey(row, pos, base_time);
		pos += 3;
		copyInRowKey(row, pos, valuetagk);
		pos += 3;
		copyInRowKey(row, pos, valuetagv);
		byte[] rowSpan = new byte[200];
		long[] rowSpanl = new long[10];
		int sizeArray = 2;
		// Timestamp to calculate opentsdb basetime. Reference from opentsdb
		Date end_date = new Date(edate);
		long end_timestamp = end_date.getTime() / 1000;
		long base_time = 64564;
		Date d1 = new Date(sdate);
		long timestamp = d1.getTime() / 1000;
		int i = 0;
		while (timestamp < end_timestamp) {
			// System.out.println("\n=======timestamp1========="+ timestamp);
			base_time = timestamp - (timestamp % 3600);
			timestamp = timestamp + 3600; // increment for next row key
			setInt(row, (int) base_time, 3);

			short post = 0;
			copyInRowKey(rowSpan, post, row);

			rowSpanl[i] = getLong(rowSpan, 0);
			rowSpanl = (long[]) resizeArray(rowSpanl, sizeArray);
			System.out.println("\n===Row Key in row span============="
					+ rowSpanl[i]);

			i++;
			sizeArray++;

		}

		for (int j = 0; j < rowSpanl.length; j++) {
			//
		}
		// return rowSpanl;

	}

	public long[][] getTimeSereiesData(String sdate, String edate,
			String tagkey, String tagvalue, String metricsval)
			throws IOException {

		HBaseConfiguration config = new HBaseConfiguration();
		HTable table = new HTable(config, "tsdb-uid");
		Get g = new Get(Bytes.toBytes(metricsval));
		Result r = table.get(g);
		byte[] value = r
				.getValue(Bytes.toBytes("id"), Bytes.toBytes("metrics"));
		// If we convert the value bytes, we should get back 'Some Value', the
		// / value we inserted at this location.
		String valueStr = Bytes.toString(value);

		byte[] family = Bytes.toBytes("id");
		byte[] qualifier = Bytes.toBytes("metrics");

		if (r != null) {
			metrics = r.getValue(family, qualifier);
		}

		// Tag K
		Get gk = new Get(Bytes.toBytes(tagkey));
		Result rk = table.get(gk);
		byte[] familyk = Bytes.toBytes("id");
		byte[] qualifierk = Bytes.toBytes("tagk");
		if (rk != null) {
			valuetagk = rk.getValue(familyk, qualifierk);
		}

		// Tag v
		Get gv = new Get(Bytes.toBytes(tagvalue));
		Result rv = table.get(gv);
		byte[] familyv = Bytes.toBytes("id");
		byte[] qualifierv = Bytes.toBytes("tagv");
		if (rv != null) {
			valuetagv = rv.getValue(familyv, qualifierv);
		}

		short pos = 0;

		final byte[] row = new byte[13];
		copyInRowKey(row, pos, metrics);
		pos += 4;
		// copyInRowKey(row, pos, base_time);
		pos += 3;
		copyInRowKey(row, pos, valuetagk);
		pos += 3;
		copyInRowKey(row, pos, valuetagv);
		byte[] rowSpan = new byte[200];
		long[] rowSpanl = new long[10];
		long[][] dataval = new long[2][5000];
		int sizeArray = 2;
		// Timestamp to calculate opentsdb basetime. Reference from opentsdb
		Date end_date = new Date(edate);
		long end_timestamp = end_date.getTime() / 1000;
		long base_time = 64564;
		Date d1 = new Date(sdate);
		long timestamp = d1.getTime() / 1000;
		int i = 0;
		while (timestamp < end_timestamp) {
			// System.out.println("\n=======timestamp1========="+ timestamp);
			base_time = timestamp - (timestamp % 3600);
			timestamp = timestamp + 3600; // increment for next row key
			setInt(row, (int) base_time, 3);

			HTable table11 = new HTable(config, "tsdb");
			Get get = new Get(row);
			Result rs = table11.get(get);
			int counter = 0;
			// System.out.println("\n================"+getLong(row,0));

			for (KeyValue kv : rs.raw()) {
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(getLong(kv.getValue(), 0));
				dataval[0][i] = kv.getTimestamp();
				dataval[1][i] = getLong(kv.getValue(), 0);
				i++;
			}

		}

		return dataval;

	}

	/****************************************************************************************************
	 * Convert String Row key to Byte array
	 */
	public static byte[] String2Bytes(String rowkey) throws Exception {
		return (Bytes.toBytes(rowkey));
	}

	/****************************************************************************************************
	 * Convert byte data to float data
	 */
	public static float Bytes2Float(byte[] data) throws Exception {
		return (Bytes.toFloat(data));
	}

	/*****************************************************************************************************
	 * Function to get base timestamp from rowkey Input parameter as Rowkey in
	 * byte. Return timestamp in long format.
	 * 
	 * @return
	 * @return
	 */
	public static int getBaseTimestamp(byte[] rowkey) throws Exception {
		byte[] timestamp = new byte[Const.TIMESTAMP_BYTES];
		if (rowkey.length >= (Const.METRICS_BYTES + Const.TIMESTAMP_BYTES)) {
			System.arraycopy(rowkey, Const.METRICS_BYTES, timestamp, 0,
					Const.TIMESTAMP_BYTES);
			int timestampNew = org.apache.hadoop.hbase.util.Bytes
					.toInt(timestamp);
			LOG.info(" Base TimeStamp in RowKey is = " + timestampNew);
			return (timestampNew);
		} else {
			LOG.info(" Error Byte of Rowkey format is smaller");
			return (Integer) null;
		}
	}

	/****************************************************************************************************
	 * Function to get Start and End rowkey also return filter parameter to
	 * filter tagk and tag v.
	 * 
	 * @param sdate
	 * @param edate
	 * @param Metrics
	 * @param tagkey
	 * @param tagvalue
	 * @return
	 * @throws Exception
	 */
	public static String[] getRowkeyFilter(String sdate, String edate,
			String metricVal, String[] tagkey, String[] tagvalue)
			throws Exception {

		// System.out.println("\n================ test ");
		// String[] tagkey = {"host"};
		// String[] tagvalue = {"bikash|foo"};
		// String sdate ="2013/05/08 06:00:00";
		@SuppressWarnings("deprecation")
		Date d1 = new Date(sdate);
		long timestamp = d1.getTime() / 1000; // To get time in millisecond
	
		 System.out.println(timestamp);
		
		// String edate="2013/05/08 08:00:00";
		@SuppressWarnings("deprecation")
		Date end_date = new Date(edate);
		long end_timestamp = end_date.getTime() / 1000;// To get time in
														// millisecond
		//System.out.println(timeStampDate);
		
		setStartTime(timestamp); // Set Start time
		setEndTime(end_timestamp); // set end time
		long end_time = getEndTime(); // get end timestamp
		final byte[] start_row = new byte[Const.METRICS_BYTES
				+ Const.TIMESTAMP_BYTES]; // 7byte array
		final byte[] end_row = new byte[Const.METRICS_BYTES
				+ Const.TIMESTAMP_BYTES];// 7byte array
		final short metric_width = Const.METRICS_BYTES;

		
		org.hbase.async.Bytes.setInt(start_row,
				(int) DataType.getScanStartTime(), metric_width);
		org.hbase.async.Bytes.setInt(end_row, (end_time == -1 ? -1 // Will scan
																	// until the
																	// end
																	// (0xFFF...).
				: (int) DataType.getScanEndTime()), 3);

		// String metricval="proc.loadavg.1m";
		byte[] metric = tsdb.metrics.getId(metricVal);
		System.arraycopy(metric, 0, start_row, 0, metric_width);
		System.arraycopy(metric, 0, end_row, 0, metric_width);
		DataType dt = new DataType();
		Map<String, String> map = new HashMap<String, String>();
		// map.put("host", "pc-0-227");
		// map.put("host", "foo");
		// map.put("host", "bikash|pc-0-227"); // tag key and tag value
		// map.put("host", "*");
		if (tagvalue.length != tagkey.length) {
			System.out.println("Error: Something wrong with parameter in tagkey and tag value. Number of size doesn't match.");
			return null;
		}
		for (int i = 1; i < tagvalue.length; i++) {
			map.put(tagkey[i], tagvalue[i]);
		}
		// getBaseTimestamp(end_row);
		dt.setTimeSeries(metricVal, map, tsdb);
		StringBuilder filter1 = dt.creatFilter(tsdb);
		String filter = filter1.toString();
		String[] data = new String[3];
		data[0] = org.apache.commons.codec.binary.Base64
				.encodeBase64String(start_row);
		data[1] = org.apache.commons.codec.binary.Base64
				.encodeBase64String(end_row);
		data[2] = org.apache.commons.codec.binary.Base64
				.encodeBase64String(Bytes.toBytes(filter));
		tsdb.shutdown();
		return data;

	}
	//********************Conversion of array of byte to float point*******************/
		public static float[] convertBytetoFloat(byte[][] arr) throws Exception
		{
			
			float data[] = null;
			LOG.info(" Bytes length" + arr.length);
			for (int i = 0; i < arr.length; i++) {
				LOG.info(" Bytes " + arr[i]);
				data[i]=Bytes2Float(arr[i]);
				LOG.info("data value =>" +data[i] +" Bytes => "+arr[i]);
			}			
			return data;	
		}
		
	//********************Conversion of array of byte to float point*******************/
	public static float[] convertBytetoFloat(String arr) throws Exception
		{
				 LOG.info(" String " + arr);
				 String[] parts = arr.split(" ");
		 			
					float data[] = null;
					/*LOG.info(" Bytes length" + arr.length);
					for (int i = 0; i < arr.length; i++) {
						LOG.info(" Bytes " + arr[i]);
						data[i]=Bytes2Float(arr[i]);
						LOG.info("data value =>" +data[i] +" Bytes => "+arr[i]);
					}*/			
					return data;	
		}
	
	//********************Conversion of array of byte to float point*******************/
		public static float[] convertBytetoFloat(double[] arr) throws Exception
			{
					 LOG.info(" String 1 " + arr);
					float data[] = null;
				    LOG.info(" Bytes length" + arr.length);
					for (int i = 0; i < arr.length; i++) {
							LOG.info(" Bytes " + arr[i]);
							//data[i]=Bytes2Float(arr[i]);
							//LOG.info("data value =>" +data[i] +" Bytes => "+arr[i]);
						}		
						return data;	
			}
		
		//********************Conversion of array of byte to float point*******************/
				public static float[] convertBytetoFloat(byte[] bytes) throws Exception
				{
					
					float data[] = null;
					for (int i = 0; i < bytes.length; i++) {
						LOG.info(" Bytes " + bytes[i]);
						data[i]=Bytes2Float(bytes);
						LOG.info("data value =>" +data[i] +" Bytes => "+bytes[i]);
					}			
					return data;	
				}

	/****************** Function to get Hex split on different region server *********************/
	public static byte[][] getHexSplits(String startKey, String endKey,int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger
				.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger
					.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}

	/***************************************************************************************************/

}
