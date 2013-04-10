package com.ontology2.hydroxide.sortAndDedup;

import java.util.Comparator;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.files.StandardFileConstellation;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Sort;
import com.ontology2.millipede.pop.Write;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.triples.Triples;

public class SortAndDedupTriplesApp extends CommandLineApplication {

	@Override
	protected void _run(String[] arguments) throws Exception {
		if(arguments.length<1) {
			die("sortAndDedupTriples [project name]");
		}
		
		for(String project:arguments) {
			sadOneProject(project);
		}
	}

	private void sadOneProject(String project) throws Exception {
		StandardFileConstellation files = new StandardFileConstellation(project);
		
		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);	
		TripleMultiFile input=files.getDestination();
		TripleMultiFile sorted=files.getSorted();
		
		Sort<Triple> millipede = new Sort<Triple>(Triples.comparator(),new Write(sorted));
		Runner<Triple> runner = new Runner<Triple>(input,millipede);

		int nThreads=Runtime.getRuntime().availableProcessors();
		if (nThreads>4) {
			nThreads=4;
		}

		runner.setNThreads(nThreads);
		runner.run();
	}


}