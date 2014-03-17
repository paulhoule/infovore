package com.ontology2.bakemono.smushObject;

import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import com.ontology2.bakemono.rewriteSubject.RewriteSubjectOptions;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.springframework.stereotype.Component;

@Component("smushObject")
public class SmushObjectTool extends SelfAwareTool<RewriteSubjectOptions> {
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }
}
