package com.ontology2.hydroxide.turtleZero;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.source.Source;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.StringComparator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import static com.google.common.base.Preconditions.*;  

public class TurtleZero {
	RecordManager db;
	BTree table;
	BTree duplicates;
	BTree failed;
	
	LoadingCache<String,String> superCache;
	
	static Logger logger=Logger.getLogger(TurtleZero.class);
	
	static final String ID_TABLE="idTable";
	static final String DUPLICATE_TABLE="duplicateKeys";
	static final String FAILED_NAMESPACE="failedNamespace";
	
	public final static String FAILED="x";
	
	public TurtleZero() throws Exception {
		Properties dbProps=new Properties();
		dbProps.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS,"true");
		db=RecordManagerFactory.createRecordManager(PartitionsAndFiles.getTurtleZeroFile());
	
		table=createTable(ID_TABLE);
		duplicates=createTable(DUPLICATE_TABLE);
		failed=createTable(FAILED_NAMESPACE);
		
		superCache=CacheBuilder.newBuilder()
			.concurrencyLevel(8).maximumSize(100000).build(
					new CacheLoader<String,String>() {

						@Override
						public String load(String key) throws Exception {
							return _lookup(key);
						}
					}
		);
		
	}

	public void close() throws Exception {
		db.close();
	}
	
	private BTree createTable(String name) throws IOException {
		long recid=db.getNamedObject(name);
		BTree newTable;
		if (recid!=0) {
			newTable=BTree.load(db, recid);
		} else {
			newTable=BTree.createInstance(db, new StringComparator());
			db.setNamedObject(name,newTable.getRecid());
		}
		
		return newTable;
	}
	
	//
	// given the name of an entity,  return the mid
	//
	
	public String lookup(String name) throws Exception {
		if (name.startsWith("/m/"))
			return name;
		
		return superCache.get(name);		
	}
	
	private String _lookup(String name) throws Exception {		
		if(name.isEmpty())
			return "/m/01";
		
		String mid=null;
		for(String part:Splitter.on("/").split(name)) {
			if(null==mid) {
				if(!part.isEmpty()) {
					throw new Exception("Freebase identifier ["+name+"] does not start with a slash");
				}
				
				mid="/m/01";
			} else {
				mid=rawLookup(mid,part);
				if (FAILED==mid)
					return mid;
			}
		}

		return mid;
	}
	
	public Enumeration<Tuple> lookupNamespace(final String namespace) throws Exception {
		final String nsKey=namespace+"/";
		final TupleBrowser browser=table.browse(nsKey);
		
		return new Enumeration<Tuple>() {
			private Tuple nextTuple;
			
			{
				getNextTuple();		
			}

			private void getNextTuple()
					throws IOException {
				nextTuple=new Tuple();
				if(!browser.getNext(nextTuple)) {
					nextTuple=null;
				}
				String key=(String) nextTuple.getKey();
				if(!key.startsWith(nsKey)) {
					nextTuple=null;
				}
			}
			
			@Override
			public boolean hasMoreElements() {
				return null!=nextTuple;
			}

			@Override
			public Tuple nextElement() {
				Tuple result=nextTuple;
				try {
					getNextTuple();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return result;
			}
		};
	}
	
	protected String rawLookup(String namespace,String localname) throws Exception {
		checkArgument(namespace.startsWith("/m/"),"Namespace must be a mid");
		String key = namespace+"/"+localname;
		String value = (String) table.find(key);
		if (null==value) {
			return FAILED;
		}
		
		return value;
	}
	
	protected Collection<FreebaseKeyRecord> addAllKeys(Source<FreebaseKeyRecord> fkr) throws Exception {
		int count=0;
		List<FreebaseKeyRecord> failedRecords=Lists.newArrayList();
		
		while(fkr.hasMoreElements()) {
			
			FreebaseKeyRecord item = fkr.nextElement();
			if(!addKey(item)) {
				failedRecords.add(item);
			}
			count++;
			if (count % 10000 ==0) {
//				logger.info("Processed "+count+" records");
				db.commit();
			}
		}
		
		db.commit();
		return ImmutableList.copyOf(failedRecords);
	}
	
	protected boolean addKey(FreebaseKeyRecord fkr) throws Exception {
		String nsMid=null;
		try {
			nsMid=lookup(fkr.namespace);
		} catch(Exception e) { }
		
		if (nsMid==null)
			return false;
		
		String key=nsMid+"/"+fkr.localname;
		Object existing=table.insert(key,fkr.mid, false);
		if(null!=existing) {
			duplicates.insert(key, fkr.mid, false);
			logger.warn("Duplicate key,"+(new FreebaseKeyRecord.Codec()).encode(fkr));
		}
		
		return true;
	}

	public void logFails(Collection<FreebaseKeyRecord> failed2) throws Exception {
		for(FreebaseKeyRecord item:failed2) {
			failed.insert(item.namespace, "", true);
			logger.warn("Failed lookup,"+(new FreebaseKeyRecord.Codec()).encode(item));
		}	
	}
}
