package com.ontology2.hydroxide.turtleZero;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;



import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.Sink;

public class ExtractKeyRecordsApp {
	static Logger logger = Logger.getLogger(ExtractKeyRecordsApp.class);
	
	public static class ExtractKeyRecordsSegment implements  Sink<FreebaseQuad> {

		private final Sink<FreebaseKeyRecord> output;
		private final List<FreebaseKeyRecord> list;

		public ExtractKeyRecordsSegment(Sink<FreebaseKeyRecord> output) {
			this.output=output;
			this.list=new ArrayList<FreebaseKeyRecord>();
		}

		@Override
		public void accept(FreebaseQuad obj) throws Exception {
			FreebaseKeyRecord keyRec=FreebaseKeyRecord.fromQuad(obj);
			if (null!=keyRec) {
				list.add(keyRec);
			}
		}

		@Override
		public void close() throws Exception {
			Collections.sort(list);
			Plumbing.drain(list, output);
			output.close();
		}

	}

	public static class ExtractKeyRecords implements Millipede<FreebaseQuad> {

		private final MultiFile<FreebaseKeyRecord> out;

		public ExtractKeyRecords(MultiFile<FreebaseKeyRecord> out) {
			this.out=out;
		}

		@Override
		public Sink<FreebaseQuad> createSegment(int segmentNumber)
				throws Exception {
			return new ExtractKeyRecordsSegment(out.createSink(segmentNumber));
		}
	
	}

	public static void main(String[] args) throws Exception {
		MultiFile<FreebaseQuad> in=PartitionsAndFiles.getSorted();
		MultiFile<FreebaseKeyRecord> out=PartitionsAndFiles.keyFile();
		Millipede<FreebaseQuad> finder=new ExtractKeyRecords(out);
		Runner<FreebaseQuad> runner = new Runner<FreebaseQuad>(in,finder);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}
}
