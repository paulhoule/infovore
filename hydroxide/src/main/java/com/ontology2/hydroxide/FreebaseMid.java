package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Node;
import com.ontology2.basekb.BaseIRI;

//public class FreebaseMid {
//	public static String b32digits="0123456789bcdfghjklmnpqrstvwxyz_";
//
//	public static long toLong(String mid) {
//		long value=0;
//		if(!mid.startsWith("/m/0")) {
//			throw new IllegalArgumentException("ill-formed mid ["+mid+"]");						
//		}
//		
//		for(int i=4;i<mid.length();i++) {
//			String c=mid.substring(i,i+1);
//			int digitValue= b32digits.indexOf(c);
//			if (digitValue==-1) {
//				throw new IllegalArgumentException("ill-formed mid ["+mid+"]");	
//			}
//			value = value << 5;
//			value = value | digitValue;
//		}
//		
//		return value;
//	}
//	
//	public static String longToGuid(long l) {
//		return "#9202a8c04000641f"+Long.toHexString(l | 0x8000000000000000l);
//	}
//	
//	public static String midToGuid(String mid) {
//		return longToGuid(toLong(mid));
//	}
//	
//	public static Node fbToUri(String mid) {
//		return Node.createURI(BaseIRI.freebaseBase+mid.substring(1).replace("/", "."));
//	}
//}
