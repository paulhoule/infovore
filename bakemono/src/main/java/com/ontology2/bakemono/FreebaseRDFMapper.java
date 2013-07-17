package com.ontology2.bakemono;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.query.regexptrees.Text;
import com.ontology2.hydroxide.InvalidNodeException;
import com.ontology2.hydroxide.InvalidPrefixException;
import com.ontology2.millipede.Codec;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;

import org.apache.commons.logging.Log;

import static com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples.splitPrefixDeclaration;

//import static com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples.*;

public class FreebaseRDFMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
	private static org.apache.commons.logging.Log logger = LogFactory.getLog(FreebaseRDFMapper.class);
	ImmutableMap.Builder<String,String> prefixBuilder=new ImmutableMap.Builder<String,String>();
	ImmutableMap<String,String> prefixMap = ImmutableMap.of();
	Codec<PrimitiveTriple> ptCodec=new PrimitiveTripleCodec();
	
	public void declarePrefix(String obj) {
		if(obj.startsWith("@prefix")) {
			try {
				List<String> parts=splitPrefixDeclaration(obj);
				if(!prefixMap.containsKey(parts.get(1))) {
					prefixBuilder.put(parts.get(1),parts.get(2));
					prefixMap=prefixBuilder.build();
				}
			} catch(InvalidPrefixException ex) {
				logger.warn("Invalid prefix declaration: "+obj);
				return;
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		super.close();
	}

	
	@Override
	public void configure(JobConf job) {
		declarePrefix("@prefix ns: <http://rdf.freebase.com/ns/>.");
		declarePrefix("@prefix key: <http://rdf.freebase.com/key/>.");
		declarePrefix("@prefix owl: <http://www.w3.org/2002/07/owl#>.");
		declarePrefix("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
		declarePrefix("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");
		declarePrefix("@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.");		
	}

	final static Splitter lineSplitter = Splitter.on("\t").limit(3);
	final static Splitter iriSplitter = Splitter.on(":").limit(2);
	
	@Override
	public void map(LongWritable k, Text v,
			OutputCollector<Text, Text> out, Reporter meta) throws IOException {
		
		String line=v.getString();
		
		try {
			List<String> parts = expandTripleParts(line);
			accept(out,new PrimitiveTriple(parts.get(0),parts.get(1),parts.get(2)));
		} catch(InvalidNodeException ex) {
			logger.warn("Invalid triple: "+line);
//			rejectSink.accept(obj);
		}
		
		return;				
	}

	private void accept(OutputCollector<Text, Text> out,
			PrimitiveTriple primitiveTriple) throws IOException {
			out.collect(Text.create(primitiveTriple.subject), Text.create(ptCodec.encode(primitiveTriple)));
	}

	//
	// functions here have been cut-and-pasted from the old version
	// 
	//
	
	private List<String> expandTripleParts(String line) throws InvalidNodeException {
		List<String> parts=splitTriple(line);
		
		parts.set(0,expandIRINode(parts.get(0)));
		parts.set(1,expandIRINode(parts.get(1)));
		parts.set(2,expandAnyNode(parts.get(2)));
		return parts;
	}

	static List<String> splitTriple(String obj) throws InvalidNodeException {
		if (!obj.endsWith(".")) {
			throw new InvalidNodeException();
		}
		
		obj=obj.substring(0,obj.length()-1);
		List<String> parts=Lists.newArrayList(lineSplitter.split(obj));
		if (parts.size()!=3) {
			throw new InvalidNodeException();
		}
		return parts;
	}
	
	public String expandIRINode(String string) throws InvalidNodeException {
		List<String> parts=Lists.newArrayList(iriSplitter.split(string));
		if (prefixMap.containsKey(parts.get(0))) {
			return "<"+prefixMap.get(parts.get(0))+parts.get(1)+">";
		}
		
		throw new InvalidNodeException();
	}
	
	public String expandAnyNode(String string) {
		List<String> parts=Lists.newArrayList(iriSplitter.split(string));
		if (prefixMap.containsKey(parts.get(0))) {
			return "<"+prefixMap.get(parts.get(0))+parts.get(1)+">";
		}
		
		return string;
	}

}
