package com.ontology2.hydroxide.turtleThree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleTwo.ExtractTurtleTwoFactsApp.ExtractTurtleTwoFacts;
import com.ontology2.hydroxide.turtleZero.RDFGrounder;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;

public class ConstructTurtleThreeApp {

	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		String turtleTwo=PartitionsAndFiles.getTurtleTwoFile();
		Model t2Model=ModelFactory.createDefaultModel();
		t2Model.read(new FileOpener().createReader(turtleTwo),"http://rdf.basekb.com/","TURTLE");
		
		Model t3Rulebox=ModelFactory.createDefaultModel();
		String turtleThreeRulebox=PartitionsAndFiles.getTurtleThreeRuleboxFile();
		t3Rulebox.read(new FileOpener().createReader(turtleThreeRulebox),"http://rdf.basekb.com/","TURTLE");
		
		RDFGrounder grounder=new RDFGrounder(t0);
		t3Rulebox=grounder.ground(t3Rulebox);
		
		t2Model.add(t3Rulebox);
		
		MultiFile<FreebaseQuad> in=PartitionsAndFiles.getTurtleOne();
		MultiFile<Triple> out=PartitionsAndFiles.getTurtleThree();
		MultiFile<FreebaseQuad> t3rejected=PartitionsAndFiles.getTurtleThreeRejected();
		
		if(out.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Millipede<FreebaseQuad> millipede=new ConstructTurtleThree(
				new Write(out)
				, new Write(t3rejected)
				, t0,
				t2Model);
		
		Runner<FreebaseQuad> runner = new Runner<FreebaseQuad>(in,millipede);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}
}
