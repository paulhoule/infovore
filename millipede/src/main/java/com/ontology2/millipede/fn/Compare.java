package com.ontology2.millipede.fn;

//
// helper functions designed to make it easy to implement 
//

public class Compare {

	public static int cmp(long a,long b) {
		if (a>b) return 1;
		if (a<b) return -1;
		return 0;
	}
	
	// string sort with null sorting before everything else
	
	public static int cmp(String a,String b) {
		if (a==null) {
			return b==null ? 0 : -1;
		}
		
		if (b==null) return 1;
		
		return a.compareTo(b);
	}
	
	//
	// we make the value first sort in front of other things (generalize?)
	//
	
	public static int cmpFirst(String a,String b,String first) {
		if (a==null) {
			return b==null ? 0 : -1;
		}
		
		if (b==null) return 1;
		
		if (a.equals(first)) {
			return b.equals(first) ? 0 : -1;
		}
		
		if (b.equals(first)) {
			return 1;
		}
		
		return a.compareTo(b);
	}
	
	public static int chainCmp(int c1,int c2) {
		return c1==0 ? c2 : c1;
	}
	
	public static int chainCmp(int c1,int c2,int c3) {
		return chainCmp(c1,chainCmp(c2,c3));
	}
	
	public static int chainCmp(int c1,int c2,int c3,int c4) {
		return chainCmp(c1,chainCmp(c2,c3,c4));
	}
}
