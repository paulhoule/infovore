package com.ontology2.hydroxide.turtleZero;

import static com.ontology2.millipede.fn.Compare.chainCmp;
import static com.ontology2.millipede.fn.Compare.cmp;
import static com.ontology2.millipede.fn.Compare.cmpFirst;

import org.apache.log4j.Logger;

import com.ontology2.hydroxide.FreebaseQuad;
import static com.ontology2.basekb.StatelessIdFunctions.*;

public class FreebaseKeyRecord implements Comparable<FreebaseKeyRecord> {
	static Logger logger = Logger.getLogger(ExtractKeyRecordsApp.class);
	
	public final String namespace;
	public final String localname;
	public final String mid;
	
	public FreebaseKeyRecord(String namespace, String localname, String mid) {
		this.namespace = namespace;
		this.localname = localname;
		this.mid = mid;
		
		if (namespace.startsWith("/guid/")) {
			logger.warn("Guid identifier ["+namespace+"] found in namespace field");
		}
	}
	
	public static class Codec implements com.ontology2.millipede.Codec<FreebaseKeyRecord> {
		public String encode(FreebaseKeyRecord obj) {
			return obj.namespace+","+obj.localname+","+obj.mid;
		}

		@Override
		public FreebaseKeyRecord decode(String obj) {
			String[] parts=obj.split(",");
			return new FreebaseKeyRecord(parts[0],parts[1],parts[2]);
		}
	}

	public static FreebaseKeyRecord fromQuad(FreebaseQuad obj) {
		if (!"/type/object/key".equals(obj.getProperty())) {
			return null;
		}
		
		return new FreebaseKeyRecord(obj.getDestination(),obj.getValue(),obj.getSubject());
	}

	@Override
	public int compareTo(FreebaseKeyRecord that) {
		return chainCmp(
				cmpNamespace(namespace,that.namespace),
			    cmp(localname,that.localname),
				cmp(mid,that.mid)
		);
	}

	private static int cmpNamespace(String n1,String n2) {
		boolean isMid1=n1.startsWith("/m/");
		boolean isMid2=n2.startsWith("/m/");
		
		if(isMid1 && !isMid2) return -1;
		if(!isMid1 && isMid2) return 1;
		
		if(isMid1 && isMid2) 
			return cmp(midToLong(n1),midToLong(n2));
		
		return n1.compareTo(n2);
	}
	
	@Override
	public boolean equals(Object that) {
		if (!(that instanceof FreebaseKeyRecord))
			return false;
		
		return 0==compareTo((FreebaseKeyRecord) that);
	}
	
	
}
