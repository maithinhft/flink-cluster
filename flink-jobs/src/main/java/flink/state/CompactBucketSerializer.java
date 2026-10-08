package flink.state;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Hand-written serializer for {@link CompactBucket}: avoids Kryo and gives a versioned snapshot
 * so the state layout can evolve without breaking savepoints.
 * Layout: startMs:long, size:int, then size x (key:long, sum:double, count:long, min:double, max:double).
 */
public final class CompactBucketSerializer extends TypeSerializerSingleton<CompactBucket> {

    private static final long serialVersionUID = 1L;

    public static final CompactBucketSerializer INSTANCE = new CompactBucketSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public CompactBucket createInstance() {
        return new CompactBucket(0L);
    }

    @Override
    public CompactBucket copy(CompactBucket from) {
        return from.copy();
    }

    @Override
    public CompactBucket copy(CompactBucket from, CompactBucket reuse) {
        return from.copy();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(CompactBucket record, DataOutputView target) throws IOException {
        target.writeLong(record.startMs);
        target.writeInt(record.size);
        for (int i = 0; i < record.size; i++) {
            target.writeLong(record.keys[i]);
            target.writeDouble(record.sum[i]);
            target.writeLong(record.count[i]);
            target.writeDouble(record.min[i]);
            target.writeDouble(record.max[i]);
        }
    }

    @Override
    public CompactBucket deserialize(DataInputView source) throws IOException {
        long startMs = source.readLong();
        int size = source.readInt();
        CompactBucket b = new CompactBucket(startMs, Math.max(size, 1));
        for (int i = 0; i < size; i++) {
            b.keys[i] = source.readLong();
            b.sum[i] = source.readDouble();
            b.count[i] = source.readLong();
            b.min[i] = source.readDouble();
            b.max[i] = source.readDouble();
        }
        b.size = size;
        return b;
    }

    @Override
    public CompactBucket deserialize(CompactBucket reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        int size = source.readInt();
        target.writeInt(size);
        target.write(source, size * 40);
    }

    @Override
    public TypeSerializerSnapshot<CompactBucket> snapshotConfiguration() {
        return new Snapshot();
    }

    public static final class Snapshot extends SimpleTypeSerializerSnapshot<CompactBucket> {
        public Snapshot() {
            super(() -> INSTANCE);
        }
    }
}
