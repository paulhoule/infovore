package com.ontology2.hydroxide.extractOtherComments;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.StringComparator;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.springframework.core.io.InputStreamSource;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.freebase.Freebase;
import com.google.api.services.freebase.Freebase.Text;
import com.google.api.services.freebase.model.ContentserviceGet;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.extractOtherComments.ExtractArticleLinksApp.ExtractArticleLinks;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.pop.Write;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

//public class CrawlArticlesApp {
//
//	static Logger logger=Logger.getLogger(CrawlArticlesApp.class);
//	
//	public static class CrawlArticlesSegment implements Sink<Triple> {
//
//		final Node articlePredicate;
//		private Sink<Triple> innerSink;
//		private Freebase fbClient;
//		int count=0;
//		
//		public CrawlArticlesSegment(Sink<Triple> innerSink) throws Exception {
//			articlePredicate=toBk(t0.lookup("/common/topic/article"));	
//			this.innerSink=innerSink;
//			fbClient=new Freebase(new NetHttpTransport(),new JacksonFactory());
//		}
//		
//		@Override
//		public void accept(Triple obj) throws Exception {
//			// eat exceptions because we expect them here
//			try {
//				if (!articlePredicate.equals(obj.getPredicate())) {
//					return;
//				}
//				
//				Node subject=obj.getSubject();
//				Node target=obj.getObject();
//				
//				count++;
//				
//				String result=null;
//				synchronized(COMMENTS) {
//					result=(String) COMMENTS.find(target.toString());
//					if(null!=result) {
//						logger.info(count+" found "+target.toString()+" in local cache");
//					}
//				}
//				
//				if (null==result) {
//					result = fetchCommentFromFreebase(target);
//					synchronized(COMMENTS) {
//						COMMENTS.insert(target.toString(), result, true);
//						db.commit();
//					}
//				}
//				
//				innerSink.accept(new Triple(
//						obj.getSubject(),
//						RDFS.comment.asNode(),
//						Node.createLiteral(result,"en",false)
//				));
//
//			} catch(Exception ex) {
//				logger.error(ex.getMessage());
//			};
//		}
//
//		private String fetchCommentFromFreebase(Node target) throws Exception {
//			String requestUri="https://www.googleapis.com/freebase/v1/text"+toFb(target)+"?format=raw&key=AIzaSyDfQlfnTKik5cZqbk08RQK2OYnVvM5Xa5Q";
////			Thread.sleep(5000L);
//			HttpGet get = new HttpGet(requestUri);
//			HttpClient client = new DefaultHttpClient();
//			HttpResponse response=client.execute(get);
//			if(response.getStatusLine().getStatusCode()!=200) {
//				throw new Exception(response.getStatusLine().toString()+" for "+requestUri);	
//			}
//			
//			JSON j=JSON.parse(new InputStreamReader(response.getEntity().getContent(),"UTF-8"));
//			String result=j.get("result").string();
//			logger.info(count+" looking up "+requestUri);
//			return result;
//		}
//
//		@Override
//		public void close() throws Exception {
//			innerSink.close();
//		}
//
//	}
//
//	public static class CrawlArticles implements Millipede<Triple> {
//
//		@Override
//		public Sink<Triple> createSegment(int segmentNumber) throws Exception {
//			return new CrawlArticlesSegment(output.createSegment(segmentNumber));
//		}
//
//	}
//
//	static TurtleZero t0;
//	private static Write<Triple> output;
//	private static RecordManager db;
//	private static BTree COMMENTS;
//
//	public static void main(String[] args) throws Exception {
//		t0=new TurtleZero();
//		
//		initializeCache();
//		
//		TripleMultiFile input=PartitionsAndFiles.getMissingArticles();
//		output=new Write<Triple>(PartitionsAndFiles.getHarvestedComments());
//		Millipede<Triple> mp=new CrawlArticles();
//		Runner<Triple> r=new Runner<Triple>(input,mp);
//		r.setNThreads(1);
//		r.run();
//		db.close();
//	}
//
//	private static void initializeCache() throws IOException {
//		Properties dbProps=new Properties();
//		dbProps.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS,"true");
//		db=RecordManagerFactory.createRecordManager(PartitionsAndFiles.getCommentCacheFile());
//		COMMENTS=createTable("COMMENTS");
//	}
//	
//	// XXX -- duplicated in Turtle0
//	
//	private static BTree createTable(String name) throws IOException {
//		long recid=db.getNamedObject(name);
//		BTree newTable;
//		if (recid!=0) {
//			newTable=BTree.load(db, recid);
//		} else {
//			newTable=BTree.createInstance(db, new StringComparator());
//			db.setNamedObject(name,newTable.getRecid());
//		}
//		
//		return newTable;
//	}
//	
//}
