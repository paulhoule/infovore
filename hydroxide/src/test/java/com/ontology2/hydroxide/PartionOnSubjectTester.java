package com.ontology2.hydroxide;

import static org.junit.Assert.*;

import org.junit.Test;

public class PartionOnSubjectTester {

	@Test
	public void testGetPartitionCount() {
		PartitionOnSubject p=new PartitionOnSubject(9999);
		assertEquals(9999,p.getPartitionCount());
	}

	@Test
	public void testBin1() {
		PartitionOnSubject p=new PartitionOnSubject(9999);
		FreebaseQuad q=new FreebaseQuad("/m/0001","a","b","c");
		int bin=p.bin(q);
		assertEquals(664,bin);
	}
	
	@Test
	public void testBin2() {
		PartitionOnSubject p=new PartitionOnSubject(9999);
		FreebaseQuad q=new FreebaseQuad("/m/0002","a","b","c");
		int bin=p.bin(q);
		assertEquals(2471,bin);
	}
	

}
