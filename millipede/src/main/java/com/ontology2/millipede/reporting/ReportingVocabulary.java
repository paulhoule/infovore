package com.ontology2.millipede.reporting;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.millipede.uuid.ResourceSupplier;
import com.ontology2.millipede.uuid.UUIDSupplier;

public class ReportingVocabulary {
	private final Model that;

	public ReportingVocabulary(Model that) {
		super();
		this.that = that;
	}
	
	public Property a() {
		return that.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
	}
	
	public Property inputTripleCount() {
		return that.createProperty("http://rdf.ontology2.com/infovore/inputTripleCount");
	};
	
	public Property  outputTripleCount() {
		return that.createProperty("http://rdf.ontology2.com/infovore/outputTripleCount");
	}
	
	public Property inputCharactersCount() {
		return that.createProperty("http://rdf.ontology2.com/infovore/inputCharacterCount");
	}
	
	public Resource processingStage() {
		return that.createResource("http://rdf.ontology2.com/infovore/ProcessingStage");
	}
	
	public Property implementedBy() {
		return that.createProperty("http://rdf.ontology2.com/infovore/implementedBy");
	}
	
	public Property grosslyMalformedFacts() {
		return that.createProperty("http://rdf.ontology2.com/infovore/grosslyMalformedFacts");
	}
	
	public Property commentCount() {
		return that.createProperty("http://rdf.ontology2.com/infovore/commentCount");
	}
	
	public Resource something() {
		return new ResourceSupplier(that,new UUIDSupplier()).get();
	};
	
	public Resource asClass(Object o) {
		return that.createResource("urn:java:"+o.getClass().getCanonicalName());
	};
	
}
