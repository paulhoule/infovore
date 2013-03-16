package com.ontology2.hydroxide.cutLite;


import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.SerializedMultiFile;
import com.ontology2.hydroxide.files.PartitionsAndFiles;

import static com.ontology2.basekb.StatelessIdFunctions.*;

public class ExpandBlacklistApp {
	private static IntSet blacklist;
	private static IntSet whitelist;
	
	static Logger logger=Logger.getLogger(ExtractLinksAndLabelsApp.class);

	public static boolean isInList(IntSet list,String fbId) {
		int subjectInt=(int) midToLong(fbId);
		return list.contains(subjectInt);		
	}
	
	public static void main(String[] args) throws Exception {
		blacklist = loadCreation(PartitionsAndFiles.getBlackList());
		whitelist = loadCreation(PartitionsAndFiles.getWhiteList());
		
		System.out.println("Read lists: wl: "+whitelist.size()+" bl:"+blacklist.size());
		
		PropagateBlacklist pb=initialPropagation(
				PartitionsAndFiles.getLinkFrom(),
				PartitionsAndFiles.getLinkTo()
		);


		int pass=1;
		logger.info("Pass:"+pass+" wl: "+whitelist.size()+" bl:"+blacklist.size()+" remaining links:"+pb.linkFrom.size());
		while(pb.blacklisted>0) {
			pass++;
			PropagateBlacklist oldPb=pb;
			pb=new PropagateBlacklist();
			pb.process(pb.linkFrom,pb.linkTo);
			logger.info("Pass:"+pass+" wl: "+whitelist.size()+" bl:"+blacklist.size()+" remaining links:"+pb.linkFrom.size());			
		}
		
		new FileOpener().writeObject(
				PartitionsAndFiles.getExpandedBlackListFile(),
				blacklist.toIntArray()
		);
		
	}
	
	public static IntSet loadCreation(SerializedMultiFile<int[]> input) throws Exception {
		IntSet output=new IntOpenHashSet();
		for(int i=0;i<input.getPartitionFunction().getPartitionCount();i++) {
			IntSet part=new IntOpenHashSet(input.readFirstObject(i));
			output.addAll(part);
		}
		return output;
	}
	
	public static PropagateBlacklist initialPropagation(SerializedMultiFile<int[]> from,SerializedMultiFile<int[]> to) throws Exception {
		PropagateBlacklist pb=new PropagateBlacklist();
		for(int i=0;i<from.getPartitionFunction().getPartitionCount();i++) {
			IntList fromList=new IntArrayList(from.readFirstObject(i));
			IntList toList=new IntArrayList(to.readFirstObject(i));
			pb.process(fromList, toList);
		}		
		return pb;
	}
	
	public static class PropagateBlacklist {
		IntList linkFrom=new IntArrayList();
		IntList linkTo=new IntArrayList();
		int blacklisted=0;
		
		public void process(IntList from,IntList to) {
			for(int i=0;i<from.size();i++) {
				int fromId=from.getInt(i);
				int toId=to.getInt(i);
				
				//
				// nothing propagates to and from whitelisted nodes
				// we never visit again
				//
				
				if(whitelist.contains(fromId) || whitelist.contains(toId)) {
					continue;
				}
				
				boolean fromBlacklist=blacklist.contains(fromId);
				boolean toBlacklist=blacklist.contains(toId);
				
				if (fromBlacklist && toBlacklist)
					continue;
				
				if (fromBlacklist) {
					blacklist(toId);
					continue;
				}
				
				if (toBlacklist) {
					blacklist(fromId);
					continue;
				}
				
				// if we haven't followed the link,  copy it to the list to go again.
				
				linkFrom.add(fromId);
				linkTo.add(toId);
			}
		}

		private void blacklist(int toId) {
			blacklist.add(toId);
//			System.out.println("blacklisting "+FreebaseMid.longToGuid(toId));
			blacklisted++;
		}
	}
}

