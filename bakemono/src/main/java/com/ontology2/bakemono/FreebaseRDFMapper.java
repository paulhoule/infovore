package com.ontology2.bakemono;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ontology2.hydroxide.InvalidNodeException;
import com.ontology2.hydroxide.InvalidPrefixException;
import com.ontology2.millipede.Codec;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriplePredicateRewriter;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleReverser;
import com.ontology2.millipede.sink.Sink;

import org.apache.commons.logging.Log;

//import static com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples.*;

public class FreebaseRDFMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
	private static org.apache.commons.logging.Log logger = LogFactory.getLog(FreebaseRDFMapper.class);
	ImmutableMap.Builder<String,String> prefixBuilder=new ImmutableMap.Builder<String,String>();
	ImmutableMap<String,String> prefixMap = ImmutableMap.of();
	Codec<PrimitiveTriple> ptCodec=new PrimitiveTripleCodec();
	private Predicate<PrimitiveTriple> tripleFilter;
	private Function<PrimitiveTriple, PrimitiveTriple> rewritingFunction;
	
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

	
//	Predicate<PrimitiveTriple> tripleFilter
	
	@Override
	public void configure(JobConf job) {
		declarePrefix("@prefix ns: <http://rdf.freebase.com/ns/>.");
		declarePrefix("@prefix key: <http://rdf.freebase.com/key/>.");
		declarePrefix("@prefix owl: <http://www.w3.org/2002/07/owl#>.");
		declarePrefix("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
		declarePrefix("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");
		declarePrefix("@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.");		
		
		
		tripleFilter=acceptTheseTriples();
		rewritingFunction=tripleRewritingFunction();
		
	}

	final static Splitter lineSplitter = Splitter.on("\t").limit(3);
	final static Splitter iriSplitter = Splitter.on(":").limit(2);
	private Sink<PrimitiveTriple> rewriter;
	
	@Override
	public void map(LongWritable k, Text v,
			OutputCollector<Text, Text> out, Reporter meta) throws IOException {
		
		String line=v.toString();
		if (line.startsWith("@prefix")) {
			meta.incrCounter(FreebasePrefilterCounter.PREFIX_DECL, 1);
			return;
		}
		
		try {
			List<String> parts = expandTripleParts(line);
			line.getBytes();
			PrimitiveTriple triple=new PrimitiveTriple(parts.get(0),parts.get(1),parts.get(2));

			
			if(tripleFilter.apply(triple)) {
				triple=rewritingFunction.apply(triple);
				accept(out,triple);
				meta.incrCounter(FreebasePrefilterCounter.ACCEPTED,1);
			} else {
				meta.incrCounter(FreebasePrefilterCounter.IGNORED, 1);
			}
			

		} catch(InvalidNodeException ex) {
			meta.incrCounter(FreebasePrefilterCounter.ILL_FORMED,1);
			logger.warn("Invalid triple: "+line);
//			rejectSink.accept(obj);
		}
		
		return;				
	}

	
	private void accept(OutputCollector<Text, Text> out,
			PrimitiveTriple primitiveTriple) throws IOException {
			out.collect(new Text(primitiveTriple.subject), new Text(primitiveTriple.poPairAsString()));
	}

	//
	// functions here have been cut-and-pasted from the old version
	// 
	//
	
	List<String> expandTripleParts(String line) throws InvalidNodeException {
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
	
	public static List<String> splitPrefixDeclaration(String obj) throws InvalidPrefixException {
		List<String> parts=Lists.newArrayList(Splitter.on(" ").split(obj));
		if (parts.size()!=3) {
			throw new InvalidPrefixException();
		}
		
		String prefix=parts.get(1);
		String mapsTo=parts.get(2);	
		
		if (!prefix.endsWith(":")) {
			throw new InvalidPrefixException();
		}
		
		parts.set(1, prefix.substring(0, prefix.length()-1));

		if (!mapsTo.startsWith("<") || !mapsTo.endsWith(">.")) {
			throw new InvalidPrefixException();
		}
		
		parts.set(2, mapsTo.substring(1, mapsTo.length()-2));
		
		return parts;
	} 
	
	public static Predicate <PrimitiveTriple> acceptTheseTriples() {
		return Predicates.not(Predicates.or(
				PrimitiveTriple.hasPredicate("<http://rdf.freebase.com/ns/type.type.instance>"),
				PrimitiveTriple.hasPredicate("<http://rdf.freebase.com/ns/type.type.expected_by>"),
				Predicates.and(
						PrimitiveTriple.hasPredicate("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
						PrimitiveTriple.objectMatchesPrefix("<http://rdf.freebase.com")
				)
		));
	}


	public static Function<PrimitiveTriple, PrimitiveTriple> tripleRewritingFunction() {
		return Functions.compose(Functions.compose(
		new PrimitiveTripleReverser(
				"<http://rdf.freebase.com/ns/type.permission.controls>"
				,"<http://rdf.freebase.com/ns/m.0j2r9sk>")
		,new PrimitiveTripleReverser(
				"<http://rdf.freebase.com/ns/dataworld.gardening_hint.replaced_by>"
				,"<http://rdf.freebase.com/ns/m.0j2r8t8>"))
		,new PrimitiveTriplePredicateRewriter(
				"<http://rdf.freebase.com/ns/type.object.type>",
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
	}

	
}
