package com.ontology2.millipede.source;

import java.util.List;

import org.junit.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.ontology2.millipede.Plumbing;

import static org.junit.Assert.*;

public class OrderedMergeSourceTester {
	
	@Test
	public void test0001() throws Exception {
		Source<String> source1=new StoredValueSource(
				Lists.newArrayList("a","c")
		);
		
		Source<String> source2=new StoredValueSource(
				Lists.newArrayList("b","d")
		);
	
		List<Source<String>> sources=Lists.newArrayList(source1,source2);
		OrderedMergeSource merge=new OrderedMergeSource(sources,Ordering.natural());
		List<String> output=Plumbing.toList(merge);
		
		assertEquals(4,output.size());
	}
	
	@Test
	public void test0002() throws Exception {
		Source<String> source1=new StoredValueSource(
				Lists.newArrayList("a","b")
		);
		
		Source<String> source2=new StoredValueSource(
				Lists.newArrayList("c","d")
		);
	
		List<Source<String>> sources=Lists.newArrayList(source1,source2);
		OrderedMergeSource merge=new OrderedMergeSource(sources,Ordering.natural());
		List<String> output=Plumbing.toList(merge);
		
		assertEquals(4,output.size());
	}
}
