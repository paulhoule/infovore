package com.ontology2.hydroxide.rdfMerger;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Node;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.MultiSource;
import com.ontology2.millipede.MultiSourceMerger;
import com.ontology2.millipede.PullMultiSource;
import com.ontology2.millipede.PullTransformer;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.TransformingSource;

//
// Note we are "merging" several N-Triples files to make a N-Quads file where all the
// triples are in a named graph
//

public class RDFMerger  {

	public static MultiSource<String> create(List<MergeSource> sources) {
		List<PullMultiSource<String>> lineSources=Lists.newArrayList();
		for(MergeSource f:sources) {
			final TripleMultiFile triples=f.source;
			final String graph=f.targetGraph.toString();
			lineSources.add(new PullTransformer<String,String>(triples.getLines(),new Function<String,String>() {

				@Override
				public String apply(String input) {
					// Hack off "." at end of N-Triple
					String statement=input.substring(0,input.length()-2);
					return statement+" <"+graph+"> .";
				}}));
		}
		
		return new MultiSourceMerger<String>(lineSources,new SortLineOnSubject());
	}

	public static class MergeSource {
		public final TripleMultiFile source;
		public final Node targetGraph;
		
		public MergeSource(TripleMultiFile source, Node targetGraph) {
			this.source = source;
			this.targetGraph = targetGraph;
		}
	}
}
