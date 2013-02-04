package com.ontology2.hydroxide;

import static org.junit.Assert.*;

import org.junit.Test;


public class FreebaseMidTester {
	
	@Test
	public void sortTest1() {
		FreebaseQuad q1=new FreebaseQuad("/m/01","a",null,"b");
		FreebaseQuad q2=new FreebaseQuad("/m/02","a",null,"b");
		int result=new QuadComparator().compare(q1, q2);
		assertEquals(-1,result);
	}
	
}
