package com.ontology2.hydroxide.turtleThree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Write;
import com.ontology2.millipede.sink.Sink;

public class ConstructTurtleThree implements Millipede<FreebaseQuad> {

	private final Millipede<Triple> output;
	private final Model turtleTwo;
	private final TurtleZero t0;
	private final Millipede<FreebaseQuad> rejected;

	public ConstructTurtleThree(Millipede<Triple> output,Millipede<FreebaseQuad> rejected,TurtleZero t0, Model turtleTwo) {
		this.output = output;
		this.t0 = t0;
		this.turtleTwo = turtleTwo;
		this.rejected = rejected;
	}

	@Override
	public Sink<FreebaseQuad> createSegment(int segmentNumber) throws Exception {
		return new ConstructTurtleThreeSegment(
				output.createSegment(segmentNumber),
				rejected.createSegment(segmentNumber),
				t0,
				turtleTwo);
	}

}
