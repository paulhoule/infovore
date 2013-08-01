package com.ontology2.hydroxide;

import java.io.BufferedReader;
import java.io.Reader;

import com.google.common.base.CharMatcher;
import com.ontology2.millipede.FileOpener;

public class FindUnicodeLinesApp {
    public static void main(String[] argv) throws Exception {
        String filename="Y:/freebase/2012-03-08/sortedKnownAs/triples0001.nt";
        BufferedReader r=new FileOpener().createBufferedReader(filename);

        while(true) {
            String line=r.readLine();
            if (null==line)
                break;

            if(!CharMatcher.ASCII.matchesAllOf(line)) {
                System.out.println(line);
            }
        }
    }
}
