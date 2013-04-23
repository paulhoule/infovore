package com.ontology2.millipede.reporting;
import java.io.File;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
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
	
	protected Property property(String url) {
		return that.createProperty("http://rdf.ontology2.com/infovore/"+url);
	}
	
	protected Resource resource(String url) {
		return that.createResource("http://rdf.ontology2.com/infovore/"+url);
	}
	
	public Property inputTripleCount() {
		return property("inputTripleCount");
	};
	
	public Property  outputTripleCount() {
		return property("outputTripleCount");
	}
	
	public Property inputCharactersCount() {
		return property("inputCharacterCount");
	}
	
	public Resource processingStage() {
		return resource("ProcessingStage");
	}
	
	public Resource Job() {
		return resource("Job");
	}
	
	public Property flowsTo() {
		return property("flowsTo");
	}
	
	public Property implementedBy() {
		return property("implementedBy");
	}
	
	public Property grosslyMalformedFacts() {
		return property("grosslyMalformedFacts");
	}
	
	public Property commentCount() {
		return property("commentCount");
	}
	
	public Resource something() {
		return new ResourceSupplier(that,new UUIDSupplier()).get();
	};
	
	public Resource asClass(Object o) {
		return that.createResource("urn:java:"+o.getClass().getCanonicalName());
	};

	public Resource File() {
		return resource("File");
	}

	public Property path() {
		return property("fromPath");
	}

	public Resource file(File file) {
		return that.createResource(file.toURI().toString());
	}

	public Property fromInstance() {
		return property("fromInstance");
	}

	public Property prefixDeclCount() {
		return property("prefixDeclCount");
	}

	public Property rawAcceptedCount() {
		return property("rawAcceptedCount");
	}

	public Property refusedCount() {
		return property("refusedCount");
	}
	
	public Property isThe() {
		return property("isThe");
	}
	
	public Resource SubjectOfThisGraph() {
		return resource("SubjectOfThisGraph");
	}

}
