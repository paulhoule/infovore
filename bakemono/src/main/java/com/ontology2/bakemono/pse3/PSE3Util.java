package com.ontology2.bakemono.pse3;

import java.util.regex.Pattern;

public class PSE3Util {
    private final static Pattern dateTimePattern=Pattern.compile("-?\\d{4,}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}([.]\\d{1,})?(Z|[+-]?\\d{2}:\\d{2})?");
    public static Pattern dateTimePattern() {
        return dateTimePattern;
    }
}
