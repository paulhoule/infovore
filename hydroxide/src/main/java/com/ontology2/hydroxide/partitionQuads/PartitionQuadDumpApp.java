package com.ontology2.hydroxide.partitionQuads;


import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionOnSubject;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.PropertyIs;
import com.ontology2.hydroxide.QuadReverser;
import com.ontology2.hydroxide.SubjectIs;
import com.ontology2.millipede.Codec;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.sink.FilterSink;
import com.ontology2.millipede.sink.ProgressReportingSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class PartitionQuadDumpApp {

	public static void main(String[] args) throws Exception {
		PartitionOnSubject partitionFunction=new PartitionOnSubject(1024);
		SingleFileSource<FreebaseQuad> source=PartitionsAndFiles.getRawQuads();
		MultiFile<FreebaseQuad> mf=PartitionsAndFiles.getPartioned();
		
		if(mf.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Partitioner<FreebaseQuad> p=new Partitioner<FreebaseQuad>(mf);
		Predicate<FreebaseQuad> quadFilter=(Predicate<FreebaseQuad>) Predicates.not(
				Predicates.or(
						new PropertyIs("/type/type/expected_by"),
						new PropertyIs("/type/type/instance"),
						Predicates.and(new PropertyIs("/type/permission/controls"),new SubjectIs("/m/049"))
				));
		
		
		Sink<FreebaseQuad> sink=new QuadReverser(p,"/type/permission/controls","/m/0j2r9sk");
		sink=new QuadReverser(sink,"/dataworld/gardening_hint/replaced_by","/m/0j2r8t8");
		sink=new FilterSink<FreebaseQuad>(sink, quadFilter);
		Plumbing.flow(source,new ProgressReportingSink<FreebaseQuad>(sink));
	}

}
