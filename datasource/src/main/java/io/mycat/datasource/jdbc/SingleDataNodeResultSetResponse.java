package io.mycat.datasource.jdbc;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.MySQLPacketUtil;
import java.io.IOException;
import java.util.Iterator;

public class SingleDataNodeResultSetResponse implements MycatResultSetResponse {

  final RowBaseIterator rowBaseIterator;

  public SingleDataNodeResultSetResponse(RowBaseIterator rowBaseIterator) {
    this.rowBaseIterator = rowBaseIterator;
  }

  @Override
  public int columnCount() {
    return rowBaseIterator.metaData().getColumnCount();
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return new Iterator<byte[]>() {
      final int count = SingleDataNodeResultSetResponse.this.columnCount();
      int index = 1;

      @Override
      public boolean hasNext() {
        return index <= count;
      }

      @Override
      public byte[] next() {
        return MySQLPacketUtil
            .generateColumnDefPayload(
                SingleDataNodeResultSetResponse.this.rowBaseIterator.metaData(),
                index++);
      }
    };
  }

  @Override
  public Iterator<byte[]> rowIterator() {
    return new Iterator<byte[]>() {
      final int count = SingleDataNodeResultSetResponse.this.columnCount();

      @Override
      public boolean hasNext() {
        return rowBaseIterator.next();
      }

      @Override
      public byte[] next() {
        //todo optimize to remove tmp array
        RowBaseIterator rowBaseIterator = SingleDataNodeResultSetResponse.this.rowBaseIterator;
        byte[][] bytes = new byte[count][];
        for (int i = 0, j = 1; i < count; i++, j++) {
          bytes[i] = rowBaseIterator.getBytes(j);
        }
        return MySQLPacketUtil.generateTextRow(bytes);
      }
    };
  }

  @Override
  public void close() throws IOException {
    rowBaseIterator.close();
  }
}