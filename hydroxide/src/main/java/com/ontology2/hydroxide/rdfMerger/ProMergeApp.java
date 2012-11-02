package com.ontology2.hydroxide.rdfMerger;

import java.util.List;

import com.google.common.collect.Lists;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.rdfMerger.RDFMerger.MergeSource;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.MultiSource;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class ProMergeApp {
	public static void main(String[] args) throws Exception {
		List<MergeSource> sources=Lists.newArrayList();
		sources.add(
				new RDFMerger.MergeSource(
						PartitionsAndFiles.getTurtleThree()
						,Node.createURI("http://rdf.basekb.com/internal/turtle3Graph")));

		sources.add(
				new RDFMerger.MergeSource(
						PartitionsAndFiles.getSortedComments()
						,Node.createURI("http://rdf.basekb.com/internal/commentGraph")));
		
		sources.add(
				new RDFMerger.MergeSource(
						PartitionsAndFiles.getSortedHarvestedComments()
						,Node.createURI("http://rdf.basekb.com/internal/harvestedCommentGraph")));
		
		sources.add(
				new RDFMerger.MergeSource(
						PartitionsAndFiles.getSortedKnownAs(), 
						Node.createURI("http://rdf.basekb.com/internal/knownAsGraph")));
		
		MultiSource<String> merger=RDFMerger.create(sources);
		Runner r=new Runner(merger,new Write(PartitionsAndFiles.getProInput().getLines()));
		r.setNThreads(PartitionsAndFiles.getNThreads());
		r.run();
	}

}
