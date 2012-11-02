package com.ontology2.hydroxide.rdfMerger;

import java.util.Comparator;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.graph.Node;
import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

import static com.ontology2.millipede.fn.Compare.*;

public class SortLineOnSubject implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		long s1=getOrderId(o1);
		long s2=getOrderId(o2);
		
		return cmp(s1,s2);
	}

	private long getOrderId(String o1) {
		return midToLong(getMid(getSubject(o1)));
	}
	
	static String getSubject(String line) {
		String subject=Iterables.getFirst(Splitter.on(' ').trimResults(CharMatcher.anyOf("<>")).split(line),null);
		return subject;
	}
	
	static String getMid(String subjectUri) {
		Node n=Node.createURI(subjectUri);
		String mid=toFb(n);
		return mid;
	}
}
