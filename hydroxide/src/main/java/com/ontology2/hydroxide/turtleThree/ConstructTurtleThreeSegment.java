package com.ontology2.hydroxide.turtleThree;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.common.base.Ascii;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.iri.impl.IRIFactoryImpl;
import com.hp.hpl.jena.iri.impl.IRIImpl;
import com.hp.hpl.jena.n3.IRIResolver;
import com.hp.hpl.jena.n3.JenaURIException;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;
import com.ontology2.hydroxide.BKBInternal;
import com.ontology2.hydroxide.BKBPublic;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.turtleZero.ConstructTurtleZeroApp;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class ConstructTurtleThreeSegment implements Sink<FreebaseQuad> {
	public class InvalidQuadException extends Exception {

		public InvalidQuadException(String string) {
			super(string);
		}

	}

	static Logger logger=Logger.getLogger(ConstructTurtleThreeSegment.class);
	
	public class ProcessText implements Sink<FreebaseQuad> {

		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			Node language=toBk(obj.getDestination());
			Statement langTag=turtleTwo.getProperty(
					turtleTwo.createResource(language.toString()),
					turtleTwo.createProperty(BKBInternal.hasLangTag.toString()));
			
			if (null==langTag) {
				logger.warn("Quad found with invalid language tag ["+language+"]");
				output.accept(new Triple(toBk(obj.getSubject())
						, rewriteProperty(obj.getProperty())
						, Node.createLiteral(obj.getValue())));
				return;
			}
			
			String langId=langTag.getObject().toString();
			output.accept(new Triple(toBk(obj.getSubject())
					, rewriteProperty(obj.getProperty())
					, Node.createLiteral(obj.getValue(), langId, false)));
		
		}

		@Override
		public void close() throws Exception {
		}
	}

	public class ProcessObject implements Sink<FreebaseQuad> {

		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getValue().isEmpty()) {
				throw new InvalidQuadException("Value field is not empty for object quad: "+obj.toString());
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					toBk(obj.getDestination())
			));

		}

		@Override
		public void close() throws Exception {
		}
	}

	
	public class ProcessBoolean implements Sink<FreebaseQuad> {

		final Set<String> possibleValues=Sets.newHashSet("true","false");
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for boolean quad: "+obj.toString());
			}
			
			if (!possibleValues.contains(obj.getValue())) {
				throw new InvalidQuadException("Boolean quad with invalid value field: "+obj.toString());
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					Node.createLiteral(obj.getValue(), XSDDatatype.XSDboolean)
			));

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessUri implements Sink<FreebaseQuad> {
		final IRIResolver resolver=new IRIResolver();
		
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for uri quad: "+obj.toString());
			}
			
			String target=obj.getValue();
	        try { target = resolver.resolve(target) ; }
	        catch (JenaURIException ex) { throw new InvalidQuadException("Invalid IRI in quad ["+target+"]") ; }
	        
	        if(target.contains("\\") 
	        		|| target.contains("{") 
	        		|| target.contains(" ") 
	        		|| target.contains("}")
	        		|| target.contains("<")
	        		|| target.contains(">")
	        		|| target.contains("|")
	        		|| target.contains("^")
	        		|| target.contains("`")
	        		|| target.contains("\"")
	        		|| !CharMatcher.ASCII.matchesAllOf(target)
	        	) {
	        	throw new InvalidQuadException("Invalid IRI in quad ["+target+"]");			
	        }
	        
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					Node.createURI(obj.getValue())
			));

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessDatetime implements Sink<FreebaseQuad> {
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for datetime quad: "+obj.toString());
			}
			
			Node dateTime=FbDateConversion.convertFreebaseDate(obj.getValue());
			
			if (!FbDateConversion.isValidDate(dateTime)) {
				throw new InvalidQuadException("Invalid date value: "+obj.getValue().toString());				
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					dateTime)
			);

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessRawstring implements Sink<FreebaseQuad> {
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for rawstring quad: "+obj.toString());
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					Node.createLiteral(obj.getValue())
			));

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessFloat implements Sink<FreebaseQuad> {
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for float quad: "+obj.toString());
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					Node.createLiteral(obj.getValue(),XSDDatatype.XSDdouble)
			));

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessInt implements Sink<FreebaseQuad> {
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			if(!obj.getDestination().isEmpty()) {
				throw new InvalidQuadException("Destination field is not empty for integer quad: "+obj.toString());
			}
			
			output.accept(new Triple(
					toBk(obj.getSubject()),
					rewriteProperty(obj.getProperty()),
					Node.createLiteral(obj.getValue(),XSDDatatype.XSDlong)
			));

		}

		@Override
		public void close() throws Exception {
		}
	}
	
	public class ProcessKey implements Sink<FreebaseQuad> {

		private final Node namespace;
		private final Node value;
		
		public ProcessKey() throws Exception {
			namespace=toBk(t0.lookup("/type/key/namespace"));
			value=toBk(t0.lookup("/type/value/value"));
		}
		
		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			String destination = obj.getDestination().isEmpty() ? "/m/01" : obj.getDestination();

			output.accept(new Triple(
					toBk(obj.getSubject()),
					toBk(destination),
					Node.createLiteral(unescapeKey(obj.getValue()))
			));
		}

		@Override
		public void close() throws Exception {
		}
	}
	
	
	private final Sink<Triple> output;
	private final Model turtleTwo;
	private final TurtleZero t0;
	private Map<String,Sink<FreebaseQuad>> quadProcessors;
	private final Sink<FreebaseQuad> rejected;

	public ConstructTurtleThreeSegment(Sink<Triple> createSegment,
			Sink<FreebaseQuad> rejected, TurtleZero t0, Model turtleTwo) throws Exception {
		this.output=createSegment;
		this.rejected=rejected;
		
		this.t0=t0;
		this.turtleTwo=turtleTwo;

		
		quadProcessors=Maps.newHashMap();
		quadProcessors.put("text", new ProcessText());
		quadProcessors.put("object", new ProcessObject());
		quadProcessors.put("key", new ProcessKey());
		quadProcessors.put("boolean", new ProcessBoolean());
		quadProcessors.put("uri", new ProcessUri());
		quadProcessors.put("datetime", new ProcessDatetime());
		quadProcessors.put("rawstring", new ProcessRawstring());
		quadProcessors.put("float", new ProcessFloat());
		quadProcessors.put("int", new ProcessInt());
	}

	@Override
	public void accept(FreebaseQuad obj) throws Exception {
		Resource property=(Resource) toBk(turtleTwo,obj.getProperty());
		
		if(turtleTwo.contains(
				property,
				RDF.type,
				turtleTwo.asRDFNode(BKBInternal.SuppressedProperty))) {
			return;
		};
		
		Statement typeS=turtleTwo.getProperty(
				property,
				RDFS.range);
		
		String type="object";
		if (typeS!=null) {
			Resource range=typeS.getObject().asResource();
			Statement codeS=turtleTwo.getProperty(
					range,
					turtleTwo.createProperty(BKBInternal.uses.toString()));
			
			if (null!=codeS) {
				type=codeS.getObject().toString();
			}
		}
		
		if(quadProcessors.containsKey(type)) {
			try {
				quadProcessors.get(type).accept(obj);
				return;
			} catch(InvalidQuadException e) {
				logger.warn(e.getMessage());
			}
		}

		
		rejected.accept(obj);
	}
	
	protected Node rewriteProperty(String property) {
		Resource r=(Resource) toBk(turtleTwo,property);
		Statement rewriteS=turtleTwo.getProperty(r,turtleTwo.createProperty(BKBInternal.rewrittenAs.toString()));
		if (null!=rewriteS) {
			return rewriteS.getObject().asNode();
		}
		
		return r.asNode();
	}

	@Override
	public void close() throws Exception {
		output.close();
		rejected.close();
		for(Sink<FreebaseQuad> qp:quadProcessors.values()) {
			qp.close();
		}
	}
	

}
