package com.ontology2.hydroxide.assembler;

import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.Quad;
import com.ontology2.hydroxide.StatefulNQuadsFilter;
import com.ontology2.hydroxide.StatefulTripleFilter;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Write;
import com.ontology2.millipede.sink.Sink;

public class Assembler implements Millipede<Quad> {

	public class AssemblerSegment extends StatefulNQuadsFilter  {

		public AssemblerSegment(Sink<Triple> innerSink) {
			super(innerSink);
		}

		@Override
		protected void closeGroup() throws Exception {
			Model output=ModelFactory.createDefaultModel();
			Dataset input=DatasetFactory.create(model);
			
			Resource subject=(Resource) output.asRDFNode((Node) getGroupKey());
			for(AssemblerStep step:steps) {
				step.applyRule(subject,input, output);
			}
			
			Plumbing.drainTo(output,innerSink);
		}

	}

	private final List<AssemblerStep> steps;
	private final Millipede<Triple> output;

	public Assembler(List<AssemblerStep> steps, Millipede<Triple> output) {
		this.steps=steps;
		this.output=output;
	}

	@Override
	public Sink<Quad> createSegment(int segmentNumber) throws Exception {
		return new AssemblerSegment(output.createSegment(segmentNumber));
	}

}
