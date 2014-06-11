/**************************
 * Author:Bikash Agrawal
 * Email: er.bikash21@gmail.com
 * Created: 10 May 2013
 * Website: www.bikashagrawal.com.np
 * 
 * Description: This class define result set type RHResult. It is an array of column and cell values.
 */
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.RHBytesWritable;
import org.godhuli.rhipe.RHNull;
import org.godhuli.rhipe.RObjects;
import org.hbase.async.Bytes;

public class RHResult extends RHBytesWritable {
    private Result _result;
    private static REXP template;
    private static String[] _type = new String[]{};
    private final Log LOG = LogFactory.getLog(RHResult.class);

    REXP.Builder template(){
    	return( REXP.newBuilder(template));
    }

    {
    	REXP.Builder templatebuild  = REXP.newBuilder();
    	templatebuild.setRclass(REXP.RClass.LIST);
    	template = templatebuild.build();
    } 
    public RHResult(){
    	super();
    }
    public void set(Result r){
    	makeRObject(r);
    }
    public void makeRObject(Result r){
		if(r == null) {
		    super.set(RHNull.getRawBytes());
		    return;
		}
		NavigableMap<byte[],NavigableMap<byte[],byte[]>> map = r.getNoVersionMap();
		ArrayList<String> names = new ArrayList<String>();
		REXP.Builder b = REXP.newBuilder(template);
		for(Map.Entry<byte[] , NavigableMap<byte[],byte[]> > entry: map.entrySet()){
			String family = new String(entry.getKey());
		    for(Map.Entry<byte[], byte[]> columns : entry.getValue().entrySet()){
			    byte[] qual = columns.getKey();
			    //String column = new String(columns.getKey());
			    short delta = 0;
			    delta = (short) (( org.apache.hadoop.hbase.util.Bytes.toShort(qual) & 0xFFFF) >>> 4); // calculate delta in column qualifier
			    names.add(family + ":" +delta);
			    //names.add( family +":"+column);
				REXP.Builder thevals   = REXP.newBuilder();
				thevals.setRclass(REXP.RClass.RAW);
				thevals.setRawValue(com.google.protobuf.ByteString.copyFrom( columns.getValue() ));
				b.addRexpValue( thevals.build() );
		    }
		}
		b.addAttrName("names");
		b.addAttrValue(RObjects.makeStringVector(names.toArray(_type)));
		super.set(b.build().toByteArray()); //return bytes array for column qualifier and value
    }
}
