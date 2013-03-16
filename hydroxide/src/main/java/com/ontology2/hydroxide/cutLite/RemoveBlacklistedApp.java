package com.ontology2.hydroxide.cutLite;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.BKBPublic;
import com.ontology2.hydroxide.cutLite.ExtractLinksAndLabelsApp.ExtractLinksAndLabels;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.Sink;
import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class RemoveBlacklistedApp {

	public static class RemoveBlacklistedSegment implements Sink<Triple> {

		private Sink<Triple> output;

		public RemoveBlacklistedSegment(Sink<Triple> output) {
			this.output=output;
		}

		@Override
		public void accept(Triple obj) throws Exception {
			int subjectInt=(int) midToLong(toFb(obj.getSubject()));
			
			if(blacklist.contains(subjectInt)) {
				return;
			}
			
			if(obj.getObject().isURI() && !BKBPublic.knownAs.equals(obj.getPredicate())) {
				String fbId=toFb(obj.getObject());
				if (null!=fbId && blacklist.contains((int) midToLong(fbId))) {
					return;
				}
			}
			
			output.accept(obj);
		}

		@Override
		public void close() throws Exception {
			output.close();
		}

	}

	public static class RemoveBlacklisted implements Millipede<Triple> {

		@Override
		public Sink<Triple> createSegment(int segmentNumber) throws Exception {
			return new RemoveBlacklistedSegment(out.createSink(segmentNumber));
		}

	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	static IntSet blacklist;
	private static TripleMultiFile in;
	private static TripleMultiFile out;
	
	public static void main(String[] args) throws Exception {
		in = PartitionsAndFiles.getBaseKBPro();
		out = PartitionsAndFiles.getBaseKBLite();
		int[] blacklistArray=new FileOpener().readObject(PartitionsAndFiles.getExpandedBlackListFile());
		blacklist=IntSets.unmodifiable(new IntOpenHashSet(blacklistArray));
		
		System.out.println(isBlacklisted("/m/06y3r"));
		Millipede<Triple> mp=new RemoveBlacklisted();
		Runner r=new Runner(in,mp);
		r.setNThreads(PartitionsAndFiles.getNThreads());
		r.run();
	}
	
	public static boolean isBlacklisted(String fbId) {
		int subjectInt=(int) midToLong(fbId);
		
		return blacklist.contains(subjectInt);
	}

}
