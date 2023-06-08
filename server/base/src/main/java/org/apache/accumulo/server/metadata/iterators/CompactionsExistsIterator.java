package org.apache.accumulo.server.metadata.iterators;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class CompactionsExistsIterator  extends WrappingIterator {
    private static final Collection<ByteSequence> FAMS =
            Set.of(new ArrayByteSequence(ExternalCompactionColumnFamily.STR_NAME));

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException {

        Text tabletRow = LocationExistsIterator.getTabletRow(range);
        Key startKey = new Key(tabletRow, ExternalCompactionColumnFamily.NAME);
        Key endKey =
                new Key(tabletRow, ExternalCompactionColumnFamily.NAME).followingKey(PartialKey.ROW_COLFAM);

        Range r = new Range(startKey, true, endKey, false);

        super.seek(r, FAMS, true);
    }

}
