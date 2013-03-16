package com.ontology2.hydroxide.sortQuads;

import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.QuadComparator;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Sort;
import com.ontology2.millipede.pop.Write;


public class SortQuadDumpApp {
	public static void main(String[] args) throws Exception {
		MultiFile<FreebaseQuad> in=PartitionsAndFiles.getPartioned();
		MultiFile<FreebaseQuad> out=PartitionsAndFiles.getSorted();
		if(out.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Sort<FreebaseQuad> millipede = new Sort<FreebaseQuad>(new QuadComparator(),new Write(out));
		Runner<FreebaseQuad> runner = new Runner<FreebaseQuad>(in,millipede);
		
		// XXX -- prevent sporadic OOM when we've got a huge number of
		// hyperthreaded processors
		
		int nThreads=Runtime.getRuntime().availableProcessors();
		if (nThreads>4) {
			nThreads=4;
		}
		
		runner.setNThreads(nThreads);
		runner.run();
	}

}
