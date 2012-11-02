package com.ontology2.hydroxide;

import java.util.Comparator;
import static com.ontology2.millipede.fn.Compare.*;

public class QuadComparator implements Comparator<FreebaseQuad> {

	public int compare(FreebaseQuad o1, FreebaseQuad o2) {
		return chainCmp(
				cmp(o1.getSubjectAsLong(),o2.getSubjectAsLong()),
				cmpFirst(o1.getProperty(),o2.getProperty(),"/type/object/type"),
				cmp(o1.getDestination(),o2.getDestination()),
				cmp(o1.getValue(),o2.getValue())
		);
	}

}
