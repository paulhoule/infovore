package com.ontology2.hydroxide.hashId;

import static com.ontology2.basekb.StatelessIdFunctions.*;

import com.ontology2.hydroxide.files.PartitionsAndFiles;

public class HashIdApp {
	public static void main(String[] args) {
		String mid=args[0];
		if (!mid.startsWith("/m/")) {
			System.err.println("Input "+mid+"is not a mid");
			System.exit(-1);
		}
		
		int bin=hashRawMid(mid,PartitionsAndFiles.getPartitionCount());
		System.out.println(mid+ " -> bin "+bin);
	}
}
