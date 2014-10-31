package com.ontology2.bakemono.util;

import com.ontology2.bakemono.mapreduce.InputPath;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;

import java.util.List;

public class CommonOptions implements HasOptions {
    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input paths",contextualConverter=Converter.class)
    @InputPath(16)
    public List<String> input;

    @Option(description="output path",contextualConverter=Converter.class)
    public String output;

    public static class Converter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((CommonOptions) that);

            if(defaultDir.isEmpty())
                return value;

            StringBuilder there=new StringBuilder();
            there.append(defaultDir);
            if(!defaultDir.endsWith("/"))
                there.append("/");
            there.append(value);
            return there.toString();
        }

        public String getDefaultDir(CommonOptions that) {
            return that.dir;
        }
    }
}
