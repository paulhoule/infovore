package com.ontology2.bakemono.freebasePrefilter;

import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@HadoopTool("freebaseRDFPrefilter")
public class FreebaseRDFTool extends SelfAwareTool<FreebaseRDFToolOptions> {
    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        return Text.class;
    }
}
