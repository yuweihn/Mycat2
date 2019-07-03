package io.mycat.embeddedDB;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import java.util.concurrent.ThreadLocalRandom;

public class DbStartUp {

  final static String source = "CREATE TABLE `travelrecord` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
      + "\n"
      + "\n"
      + "CREATE TABLE `travelrecord2` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
      + "\n"
      + "\n"
      + "CREATE TABLE `travelrecord3` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n";

  public static void start() {
    try {
      startMySQLServer(3308);
      startMySQLServer(3309);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static DB db;

  public static void startMySQLServer(int port) throws ManagedProcessException {
    if (db == null) {
      DBConfigurationBuilder builder = DBConfigurationBuilder.newBuilder();
      builder.setDataDir("d:/tmp/" + ThreadLocalRandom.current().nextInt());
      builder.setPort(port);
      builder.addArg("--user=root");
      db = DB.newEmbeddedDB(builder.build());
      db.start();
      db.createDB("db1");
      db.run("use db1;" + source);
      db.createDB("db2");
      db.run("use db2;" + source);
      db.createDB("db3");
      db.run("use db3;" + source);
    }

  }

  public static void stop() {
    if (db!=null){
      try {
        db.stop();
      } catch (ManagedProcessException e) {
        e.printStackTrace();
      }
    }
  }
}