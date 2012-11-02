package com.ontology2.hydroxide.extractOtherComments;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.StatefulTripleFilter;
import com.ontology2.hydroxide.assembler.Grounded;
import com.ontology2.hydroxide.cutLite.ExtractLinksAndLabelsApp.ExtractLinksAndLabels;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;

public class ExtractArticleLinksApp {
	public static class ExtractArticleLinksSegment extends StatefulTripleFilter {

		final Query isCommonTopic=Grounded.query("" +
				"prefix basekb: <http://rdf.basekb.com/ns/> " +
				"ask { ?s a basekb:common.topic }");

		final Query copyArticles=Grounded.query("" +
				"prefix basekb: <http://rdf.basekb.com/ns/> " +
				"construct where { ?s basekb:common.topic.article ?o }");
		
		public ExtractArticleLinksSegment(Sink<Triple> innerSink) {
			super(innerSink);
		}

		protected void closeGroup() throws Exception {
			if(Grounded.ask(model, isCommonTopic))
				return;
			
			Model articles=Grounded.construct(model,copyArticles);
			Plumbing.drainTo(articles, innerSink);
		}
		
	}
	
	public static class ExtractArticleLinks implements Millipede<Triple> {
		@Override
		public Sink<Triple> createSegment(int segmentNumber) throws Exception {
			return new ExtractArticleLinksSegment(output.createSegment(segmentNumber));
		}
	}
	
	public static Millipede<Triple> output;
	
	public static void main(String[] args) throws Exception {
		TripleMultiFile input=PartitionsAndFiles.getTurtleThree();
		output=new Write<Triple>(PartitionsAndFiles.getMissingArticles());
		Millipede<Triple> mp=new ExtractArticleLinks();
		Runner r=new Runner(input,mp);
		r.setNThreads(PartitionsAndFiles.getNThreads());
		r.run();
	}
}
