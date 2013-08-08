package com.ontology2.chopper.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

//
// right now this expects to have maven dependency:build-classpath piped into the input

//
public class GenerateRegisterStatements {
    static List<String> forbiddenStems=Lists.newArrayList(
            "pig-",
            "hadoop-",
            "junit-",
            "mockito-",
            "xercesImpl-",
            "xml-apis-",
            "xmlenc-",
            "slf4j-"
    );
    
    public static void main(String[] argv) throws IOException {
        BufferedReader r=new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
        while(true) {
            String line=r.readLine();
            if(line==null)
                break;
            
            if(line.startsWith("["))
                continue;
            
            Iterable<String> classes=Splitter.on(":").split(line);
            for(String c:classes) {

                if(ok(c)) {
                    System.out.print("REGISTER ");
                    System.out.println(c);
                }
            }
            
            System.out.println("REGISTER /home/paul/.m2/repository/com/ontology2/chopper/2.0-SNAPSHOT/chopper-2.0-SNAPSHOT.jar");
        }
    }

    private static boolean ok(String c) {
        File f=new File(c);
        String name=f.getName();
        for(String stem:forbiddenStems) {
            if(name.startsWith(stem))
                return false;
        };
        return true;
    }
}
