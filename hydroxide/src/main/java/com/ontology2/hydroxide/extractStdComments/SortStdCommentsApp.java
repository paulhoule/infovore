package com.ontology2.hydroxide.extractStdComments;

import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.rdfMerger.SortLineOnSubject;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Sort;
import com.ontology2.millipede.pop.Write;

public class SortStdCommentsApp {

	public static void main(String[] args) throws Exception {
		MultiFile<String> in=PartitionsAndFiles.getRawComments().getLines();
		MultiFile<String> out=PartitionsAndFiles.getSortedComments().getLines();
		if(out.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Sort<String> millipede = new Sort<String>(new SortLineOnSubject(),new Write(out));
		Runner<String> runner = new Runner<String>(in,millipede);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();		
	}

}
