package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.SortedMap;

public class ColumnQualifierFilter extends  SeekingFilter {

    SortedMap<ByteSequence, NavigableSet<ByteSequence>> qualifiers;

    @Override
    public FilterResult filter(Key k, Value v) {
        // TODO could cache this set when the fam is the same beteen calls
        var qualsForFam = qualifiers.get(k.getColumnFamilyData());
        if(qualsForFam == null) {
            // qualifiers in this family are not being filtered
            return FilterResult.of(true, AdvanceResult.NEXT);
        } else {
            if(qualsForFam.contains(k.getColumnQualifierData())){
                return FilterResult.of(true, AdvanceResult.NEXT);
            } else {
                return FilterResult.of(true, AdvanceResult.USE_HINT);
            }
        }
    }

    @Override
    public Key getNextKeyHint(Key k, Value v) {
        // TODO could cache this set when the fam is the same beteen calls
        var qualsForFam = qualifiers.get(k.getColumnFamilyData());
        var ceiling = qualsForFam.ceiling(k.getColumnQualifierData());
        if(ceiling == null) {
            // go to the next possible family
            return k.followingKey(PartialKey.ROW_COLFAM);
        } else {
            // go to the next qualifier in the family
            return Key.builder().row(k.getRowData()).family(k.getColumnFamilyData()).qualifier(ceiling).build();
        }
    }

    public static IteratorSetting configure(Collection<Column> qualifiers) {
        // TODO
        return null;
    }
}
