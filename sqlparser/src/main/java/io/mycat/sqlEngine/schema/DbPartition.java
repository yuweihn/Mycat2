package io.mycat.sqlEngine.schema;

public class DbPartition {
  private final String dbMethodName;
  private final String dbPartitionCoulumn;
  private final String tableMethodName;
  private final String tablePartitionCoulumn;
  private final int tablePartitions;

  public DbPartition(String dbMethodName, String dbPartitionCoulumn, String tableMethodName,
                     String tablePartitionCoulumn, int tablePartitions) {
    this.dbMethodName = dbMethodName;
    this.dbPartitionCoulumn = dbPartitionCoulumn;
    this.tableMethodName = tableMethodName;
    this.tablePartitionCoulumn = tablePartitionCoulumn;
    this.tablePartitions = tablePartitions;
  }

  public String getDbPartitionCoulumn() {
    return dbPartitionCoulumn;
  }

  public String getTablePartitionCoulumn() {
    return tablePartitionCoulumn;
  }

  public void assignment(Object value) {

  }

  public int getReturnValue() {

    return 0;
  }
}