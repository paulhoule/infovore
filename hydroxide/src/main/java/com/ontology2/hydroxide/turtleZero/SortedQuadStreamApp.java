package com.ontology2.hydroxide.turtleZero;

import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.QuadComparator;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.source.OrderedMergeSource;
import com.ontology2.millipede.source.Source;

public class SortedQuadStreamApp {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		LineMultiFile<FreebaseQuad> in=PartitionsAndFiles.getSorted();
		Source<FreebaseQuad> merged=OrderedMergeSource.fromMultiFile(in,new QuadComparator());
		while(merged.hasMoreElements()) {
			System.out.println(merged.nextElement());
		}
	}

}
