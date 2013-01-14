package com.ontology2.hydroxide.cutLite;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ontology2.hydroxide.BKBPublic;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.SubjectGroupModelFilter;
import com.ontology2.hydroxide.turtleThree.ConstructTurtleThreeSegment;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.SerializedMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class ExtractLinksAndLabelsApp {
	
	static Logger logger=Logger.getLogger(ExtractLinksAndLabelsApp.class);
	
	public static class ExtractLinksAndLabelSegment extends SubjectGroupModelFilter {
		
		public com.hp.hpl.jena.query.Query wlQuery=QueryFactory.create(
				"prefix basekb: <http://rdf.basekb.com/ns/>" +
				"prefix public: <http://rdf.basekb.com/public/>" +
				"ASK {" +
				"   { ?s a basekb:m.0j }" +			      		// type
				"   UNION { ?s a basekb:m.02h }" +       		// property
				"   UNION { ?s a basekb:m.02c }" +       		// namespace
				"   UNION { ?s a basekb:m.0l }  " +      	 	// domain
				"   UNION { ?s a basekb:m.02m }  " +      	 	// permission (for now!)
				"   UNION { ?s basekb:m.0gt4 ?key }"+ 	// has a /wikipedia/en_id key!
				"   MINUS { ?s a basekb:m.04jsrq3 }"+ 			// not an acre application!
				"   MINUS { ?s a basekb:m.0775xx5 }"+ 			// not a /book/isbn!
				"}");
		
		public com.hp.hpl.jena.query.Query blQuery=QueryFactory.create(
				"prefix basekb: <http://rdf.basekb.com/ns/>" +
				"prefix public: <http://rdf.basekb.com/public/>" +
				"ASK {" +
				"   { ?s a basekb:m.01c5 }" +			      	// common.topic
				"   MINUS { ?s basekb:m.0gt4 ?key }"+ 	// has a /wikipedia/en_id key!
				"}");

		private Sink<int[]> lFrom;

		private Sink<int[]> lTo;

		private Sink<int[]> bl;

		private Sink<int[]> wl;

		private LinksAndLabels result=new LinksAndLabels();

		private int blCount;

		private int wlCount;

		public ExtractLinksAndLabelSegment(Sink<int[]> lFrom,
				Sink<int[]> lTo, Sink<int[]> wl,
				Sink<int[]> bl) {
			this.lFrom=lFrom;
			this.lTo=lTo;
			this.bl=bl;
			this.wl=wl;
			this.blCount=0;
			this.wlCount=0;
		}

		@Override
		public void close$() throws Exception {
			logger.info("blacklisted: "+blCount+" whitelisted: "+wlCount);
			lFrom.accept(result.fromArray.toIntArray());
			lFrom.close();
			lTo.accept(result.toArray.toIntArray());
			lTo.close();
			bl.accept(result.blackList.toIntArray());
			bl.close();
			wl.accept(result.whiteList.toIntArray());
			wl.close();
		}

		@Override
		protected void closeGroup() throws Exception {
			
			QueryExecution qe1=QueryExecutionFactory.create(wlQuery,model);
			boolean whitelisted=qe1.execAsk();
			
			QueryExecution qe2=QueryExecutionFactory.create(blQuery,model);
			boolean blacklisted=qe2.execAsk();
			
			if (blacklisted&&whitelisted) {
				logger.warn("object "+getGroupKey()+" is in both blacklist and whitelist!");
				blacklisted=false;
			}
			
			
			if (blacklisted) {
				result.addToBlackList(toFb((Node) getGroupKey()));
				blCount++;
			}
			
			if (whitelisted) {
				result.addToWhiteList(toFb((Node) getGroupKey()));
				wlCount++;
			}
			
			
			Set<LinkTuple> links=Sets.newHashSet();
			
			StmtIterator i=model.listStatements();
			while(i.hasNext()) {
				Statement s=i.next();
				if(s.getObject().isResource() && !BKBPublic.knownAs.equals(s.getPredicate().asNode())) {
					String objectMid = toFb(s.getObject());
					if (null!=objectMid) {
						links.add(new LinkTuple(toFb(s.getSubject()),objectMid));
					}
				}
			}
			
			
			result.addLinks(links); 
		}
	}

	public static class ExtractLinksAndLabels implements Millipede<Triple> {

		@Override
		public Sink<Triple> createSegment(int segmentNumber) throws Exception {
			return new ExtractLinksAndLabelSegment(
					linkFrom.createSink(segmentNumber),
					linkTo.createSink(segmentNumber),
					whitelist.createSink(segmentNumber),
					blacklist.createSink(segmentNumber)
			);
		}

	}

	public static class LinkTuple {
		public final int from;
		public final int to;
		
		public LinkTuple(String fromMid,String toMid) {
			from=(int) midToLong(fromMid);
			to=(int) midToLong(toMid);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + from;
			result = prime * result + to;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof LinkTuple))
				return false;
			LinkTuple other = (LinkTuple) obj;
			if (from != other.from)
				return false;
			if (to != other.to)
				return false;
			return true;
		}
		
		
	}
	
	//
	// you can access this in a !threadsafe way but don't
	//
	
	public static class LinksAndLabels implements Serializable {
		final IntList fromArray;
		final IntList toArray;
		
		final IntList whiteList;
		final IntList blackList;
		
		public LinksAndLabels() {
			fromArray=new IntArrayList();
			toArray=new IntArrayList();
			whiteList=new IntArrayList();
			blackList=new IntArrayList();
		}
		
		public void addLinks(Set<LinkTuple> links) {
				for(LinkTuple t:links) {
					fromArray.add(t.from);
					toArray.add(t.to);
				}
		}
		
		public void addToWhiteList(String mid) {
				whiteList.add((int) midToLong(mid)); 
		}
		
		public void addToBlackList(String mid) {
				blackList.add((int) midToLong(mid)); 
		}
		
		
	}
	
	
	static SerializedMultiFile<int[]> linkFrom;
	static SerializedMultiFile<int[]> linkTo;
	static SerializedMultiFile<int[]> whitelist;
	static SerializedMultiFile<int[]> blacklist;
		
	public static void main(String[] args) throws Exception {
		
		TripleMultiFile in=PartitionsAndFiles.getBaseKBPro();
		linkTo=PartitionsAndFiles.getLinkTo();
		linkFrom=PartitionsAndFiles.getLinkFrom();
		whitelist=PartitionsAndFiles.getWhiteList();
		blacklist=PartitionsAndFiles.getBlackList();
		
		Millipede<Triple> mp=new ExtractLinksAndLabels();
		Runner r=new Runner(in,mp);
		r.setNThreads(PartitionsAndFiles.getNThreads());
		r.run();
	}
}
