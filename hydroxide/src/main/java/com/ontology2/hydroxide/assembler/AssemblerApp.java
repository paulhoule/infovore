package com.ontology2.hydroxide.assembler;

import java.util.List;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.NQuadsMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;

public class AssemblerApp {
	
	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		
		NQuadsMultiFile coreIn=PartitionsAndFiles.getProInput();
		
		List<AssemblerStep> steps=Lists.newArrayList();
		steps.add(new CopyAllNamesForSchemaObjects());
		steps.add(new CopyEnNamespaceNames());
		steps.add(new CopyTurtleThree());
		steps.add(new ComputeGravity());
		steps.add(new CopyComments());
		steps.add(new CopyHarvestedComments());
		steps.add(new AddLabels());
		
		Millipede<Quad> assembler=new Assembler(steps,new Write<Triple>(PartitionsAndFiles.getBaseKBPro()));
		
		Runner<Quad> runner = new Runner<Quad>(coreIn,assembler);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}
}
