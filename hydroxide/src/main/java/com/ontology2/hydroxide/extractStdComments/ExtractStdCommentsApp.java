package com.ontology2.hydroxide.extractStdComments;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.source.SingleFileSource;
import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class ExtractStdCommentsApp {
	public static void main(String[] argv) throws Exception {
		SingleFileSource<String> lines=PartitionsAndFiles.getSimpleTopicDump();
		TripleMultiFile descriptions=PartitionsAndFiles.getRawComments();
		Partitioner<Triple> p=new Partitioner<Triple>(descriptions);
		
		while(lines.hasMoreElements()) {
			String line=lines.nextElement();
			String[] parts=Iterables.toArray(Splitter.on('\t').split(line),String.class);
			String mid=parts[0];
			String description=parts[5];
			
			if ("\\N".equals(description))
				continue;
			
			Triple t=new Triple(
				toBk(mid),
				RDFS.comment.asNode(),
				Node.createLiteral(cleanDescription(description))	
			);
			p.accept(t);
		}
		p.close();
	}
	
	public static String cleanDescription(String description) {
		description=description.replace("\\n", "\n");
		description=description.replace("\\t"," ");
		description=description.replace("\\"," ");
		return description;
	}
	
}
