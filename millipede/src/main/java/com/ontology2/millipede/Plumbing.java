package com.ontology2.millipede;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.LineSource;
import com.ontology2.millipede.source.Source;

public class Plumbing {
	public static <T> void flow(Source<T> input,Sink<T> output) throws Exception {
		while(input.hasMoreElements()) {
			output.accept(input.nextElement());
		}
		output.close();
	}
	
	public static <T> void flow(Iterator<T> input,Sink<T> output) throws Exception {
		while(input.hasNext()) {
			output.accept(input.next());
		}
		output.close();
	}
	
	public static <T> void fill(Source<T> input,Collection<T> target) throws Exception {
		while(input.hasMoreElements()) {
			target.add(input.nextElement());
		}
	}
	
	public static <T> List<T> toList(Source<T> input) throws Exception {
		List<T> list=Lists.newArrayList();
		fill(input,list);
		return list;
	}
	
	public static <T> void drain(Collection<T> target,Sink<T> output) throws Exception {
		for(T item:target) {
			output.accept(item);
		}
		output.close();
	}

	public static void drainTo(Model input,Sink<Triple> output) throws Exception {
		StmtIterator statements=input.listStatements();
		while(statements.hasNext()) {
			output.accept(statements.next().asTriple());
		}		
	}
	
	public static <T> void fill(Source<T> input, ImmutableSet.Builder<T> target) throws Exception {
		while(input.hasMoreElements()) {
			target.add(input.nextElement());
		}
	}
}
