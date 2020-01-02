package io.mycat.proxy;

import io.mycat.beans.resultset.MycatResultSet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ListIterator;

public class DefResultSet implements  MycatResultSet {

  private final byte[][] columnDefList;
  private final ArrayList<byte[][]> rowList;
  private final int[] jdbcTypeList;
  private final int charsetIndex;
  private final Charset charset;

  public DefResultSet(int columnCount, int charsetIndex, Charset charset) {
    this.columnDefList = new byte[columnCount][];
    this.jdbcTypeList = new int[columnCount];
    this.charsetIndex = charsetIndex;
    this.charset = charset;
    this.rowList = new ArrayList<>();

  }


  @Override
  public void addColumnDef(int index, String database, String table, String originalTable,
      String columnName, String orgName, int type, int columnFlags, int columnDecimals,
      int length) {
    byte[] bytes = MySQLPacketUtil
        .generateColumnDefPayload(database, table, originalTable, columnName,
            orgName,
            type,
            columnFlags, columnDecimals,charsetIndex, length, charset);
    columnDefList[index] = bytes;
    jdbcTypeList[index] = type;
  }

  public void addColumnDef(int index, String columnName, int type) {
    columnDefList[index] = MySQLPacketUtil
        .generateColumnDefPayload(columnName, type, charsetIndex, charset);
    jdbcTypeList[index] = type;
  }

  @Override
  public int columnCount() {
    return columnDefList.length;
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return Arrays.asList(columnDefList).iterator();
  }

  @Override
  public void addTextRowPayload(String... row) {
    byte[][] bytesArray = new byte[row.length][];
    for (int i = 0; i < row.length; i++) {
      bytesArray[i] = row[i].getBytes(charset);
    }
    addTextRowPayload(bytesArray);
  }

  @Override
  public void addTextRowPayload(byte[]... row) {
    this.rowList.add(row);
  }

  @Override
  public void addObjectRowPayload(Object[]... row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<byte[]> rowIterator() {
    ListIterator<byte[][]> iterator = rowList.listIterator();
    return new Iterator<byte[]>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public byte[] next() {
        byte[] bytes = MySQLPacketUtil.generateTextRow(iterator.next());
        iterator.set(null);
        return bytes;
      }
    };
  }

  @Override
  public void close() throws IOException {

  }
}