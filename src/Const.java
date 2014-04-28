/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 * 
 * Description: This class is used to define all the constant used to construct rowkey for timeseries data. 
 * OpenTSDB has it's own composite rowkey. With 3 bytes Metric ID+ 4 bytes base timestamp + 3 bytes tagkey ID + 3 bytes tagvalue ID +..Tag keys and values keep on appending 
 * at the end of rowkey. Normally in OpenTSDB there shoudl be at least one tag key and tagvalue.
 */


/** Constants used in various places. */
public final class Const {

  /** Number of bytes on which a timestamp is encoded. */
  public static final short TIMESTAMP_BYTES = 4;
  
  /** Number of bytes on which a Metrics is encoded. */
  public static final short METRICS_BYTES = 3;

  /** Maximum number of tags allowed per data point. */
  public static final short MAX_NUM_TAGS = 8;


  /** Number of LSBs in time_deltas reserved for flags. */
  static final short FLAG_BITS = 4;


  public static final String DATA_TABLE = "tsdb";
  public static final String LOOKUP_TABLE = "tsdb-uid";
  //public static String HBASE_CLIENT = "localhost";

  
  /**
* When this bit is set, the value is a floating point value.
* Otherwise it's an integer value.
*/
  static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier. */
  static final short LENGTH_MASK = 0x7;

  /** Mask to select all the FLAG_BITS. */
  static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;

  /** Max time delta (in seconds) we can store in a column qualifier. */
  public static final short MAX_TIMESPAN = 3600;

  /**
* Array containing the hexadecimal characters (0 to 9, A to F).
* This array is read-only, changing its contents leads to an undefined
* behavior.
*/
  public static final byte[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };

}