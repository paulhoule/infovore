package com.ontology2.bakemono.dbpediaToBaseKB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class DBpediaToBaseKBReducer extends IdentityReducer<Text,Text> {
}
