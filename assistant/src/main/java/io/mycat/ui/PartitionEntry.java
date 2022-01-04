package io.mycat.ui;

import io.mycat.BackendTableInfo;
import io.mycat.IndexBackendTableInfo;
import io.mycat.Partition;
import lombok.Data;

import java.util.Optional;


@Data
public class PartitionEntry {
    public String globalIndex;
    public String dbIndex;
    public String tableIndex;
    public String target;
    public String schema;
    public String table;

    public static PartitionEntry of(
            Integer index,
            Integer dbIndex,
            Integer tableIndex,
            String target,
            String schema,
            String table
    ) {
        PartitionEntry partitionEntry = new PartitionEntry();
        partitionEntry.setGlobalIndex(Optional.ofNullable(index).map(i -> String.valueOf(i)).orElse(null));
        partitionEntry.setDbIndex(Optional.ofNullable((dbIndex)).map(i -> String.valueOf(i)).orElse(null));
        partitionEntry.setTableIndex(Optional.ofNullable((tableIndex)).map(i -> String.valueOf(i)).orElse(null));
        partitionEntry.setSchema(schema);
        partitionEntry.setTable(table);
        partitionEntry.setTarget(target);
        return partitionEntry;
    }

    public static PartitionEntry of(
            int index,
            Partition partition
    ) {
        return of(index, 0, 0, partition.getTargetName(), partition.getSchema(), partition.getTable());
    }

    public Partition toPartition() {
        if (getTableIndex() != null) {
            return new IndexBackendTableInfo(getTarget(),
                    getSchema(),
                    getTable(),
                    Integer.parseInt(getDbIndex()),
                    Integer.parseInt(getTableIndex()),
                    Integer.parseInt(getGlobalIndex()));
        } else {
            return new BackendTableInfo(
                    getTarget(),
                    getSchema(),
                    getTable());
        }

    }
}
