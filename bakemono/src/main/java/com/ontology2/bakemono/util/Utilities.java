package com.ontology2.bakemono.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

public class Utilities {
    public static final Splitter SPACE_SPLITTER =Splitter.on(' ').omitEmptyStrings();
    public static final Splitter WHITESPACE_SPLITTER =Splitter.on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings();
    public static final Splitter SLASH_SPLITTER =Splitter.on('/').omitEmptyStrings();
}
