package com.ontology2.bakemono.rewriteSubject;

import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.springframework.stereotype.Component;

@Component("smushSubject")
public class RewriteSubjectTool extends SelfAwareTool<RewriteSubjectOptions> {
    //
    // It seems to me that it should be able to figure this out from the types involved
    //
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }
}
