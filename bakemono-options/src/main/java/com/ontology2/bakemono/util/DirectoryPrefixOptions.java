package com.ontology2.bakemono.util;

import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;

public class DirectoryPrefixOptions implements HasOptions {
    @Option(description="input and output file default directory")
    public String dir;

    public static class Converter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((DirectoryPrefixOptions) that);

            if(defaultDir.isEmpty())
                return value;

            if(value.contains(":") || value.startsWith("/")) {
                return value;
            }

            StringBuilder there=new StringBuilder();
            there.append(defaultDir);
            if(!defaultDir.endsWith("/"))
                there.append("/");
            there.append(value);
            return there.toString();
        }

        public String getDefaultDir(DirectoryPrefixOptions that) {
            return that.dir;
        }
    }
}
