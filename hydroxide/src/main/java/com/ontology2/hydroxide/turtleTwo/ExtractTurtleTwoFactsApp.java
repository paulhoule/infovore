package com.ontology2.hydroxide.turtleTwo;

import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.ontology2.hydroxide.BKBInternal;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class ExtractTurtleTwoFactsApp {

	public static class ExtractTurtleTwoFacts implements Millipede<FreebaseQuad> {

		public class ExtractTurtleTwoFactsSegment implements Sink<FreebaseQuad> {

			private final Sink<Triple> innerSink;

			public ExtractTurtleTwoFactsSegment(Sink<Triple> innerSink) {
				this.innerSink=innerSink;
			}

			@Override
			public void accept(FreebaseQuad obj) throws Exception {
				String property=obj.getProperty();
				if(propertyMap.containsKey(property)) {
					Node predicate=propertyMap.get(property);
					
					innerSink.accept(new Triple(
							toBk(obj.getSubject()), 
							predicate, 
							toBk(obj.getDestination())
					));
				};
				
				if(typeObjectKey.equals(property)) {
					if(lang.equals(obj.getDestination())) {
						innerSink.accept(new Triple(
							toBk(obj.getSubject()),
							BKBInternal.hasLangTag,
							Node.createLiteral(obj.getValue())
						));
					}
					
					if(type.equals(obj.getDestination())) {
						innerSink.accept(new Triple(
							toBk(obj.getSubject()),
							BKBInternal.hasTypeTag,
							Node.createLiteral(obj.getValue())
						));
					}
				}
			}

			@Override
			public void close() throws Exception {
				innerSink.close();
			}

		}

		private final Millipede<Triple> output;
		private final Map<String,Node> propertyMap;
		private final String typeObjectKey;
		private final String lang;
		private final String type;
		
		public ExtractTurtleTwoFacts(Millipede<Triple> output) throws Exception {
			this.output=output;
			this.propertyMap=new HashMap<String,Node>();
			addMapping("/type/property/expected_type",RDFS.Nodes.range);
			typeObjectKey=t0.lookup("/type/object/key");
			lang=t0.lookup("/lang");
			type=t0.lookup("/type");
		}
		
		protected void addMapping(String fromFreebase,Node toRDF) throws Exception {
			propertyMap.put(t0.lookup(fromFreebase), toRDF);
		};

		public Sink<FreebaseQuad> createSegment(int segmentNumber)
				throws Exception {
			return new ExtractTurtleTwoFactsSegment(output.createSegment(segmentNumber));
		}

	}

	static TurtleZero t0;
	
	public static void main(String[] args) throws Exception {
		t0=new TurtleZero();
		
		MultiFile<FreebaseQuad> in=PartitionsAndFiles.getTurtleOne();
		MultiFile<Triple> out=PartitionsAndFiles.getTurtleTwoFacts();
		if(out.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		ExtractTurtleTwoFacts millipede=new ExtractTurtleTwoFacts(new Write(out));
		Runner<FreebaseQuad> runner = new Runner<FreebaseQuad>(in,millipede);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}
}
