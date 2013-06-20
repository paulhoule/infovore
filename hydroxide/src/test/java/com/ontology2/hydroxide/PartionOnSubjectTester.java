package com.ontology2.hydroxide;

import static org.junit.Assert.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

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
	
	@Test
	public void testBin3() {
		PartitionPrimitiveTripleOnSubject p=new PartitionPrimitiveTripleOnSubject(1024);
		PrimitiveTriple q=new PrimitiveTriple("<http://dbpedia.org/resource/Tree>","b","c");
		
		assertEquals("b78f8f508982ceb4e8dd3510fac75f62",Hex.encodeHexString(DigestUtils.md5(q.subject)));
		int bin=p.bin(q);
		assertEquals(332,bin);
	}

}
