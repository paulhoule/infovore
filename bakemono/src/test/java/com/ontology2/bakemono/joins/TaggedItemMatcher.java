package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.WritableComparable;
import org.mockito.ArgumentMatcher;

public class TaggedItemMatcher<T extends WritableComparable> extends ArgumentMatcher<T> {
    private final TaggedItem<T> item;
    public TaggedItemMatcher(TaggedItem<T> item) {
        this.item=item;
    }

    @Override
    public boolean matches(Object o) {
        TaggedItem<T> that=(TaggedItem<T>) o;
        return item.getKey().equals(that.getKey())
                && item.getTag().equals(that.getTag());
    }
}
