/**
 * Copyright (C) <2022>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hint;

import io.mycat.util.JsonUtil;
import lombok.Data;

import java.text.MessageFormat;
import java.util.List;

@Data
public class BinlogSyncHint extends HintBuilder {
    String name;
    String snapshotId;
    List<String> inputTableNames;
    List<String> outputTableNames;

    private long connectTimeout;

    public static String create(String name, List<String> inputTableNames, List<String> outputTableNames) {
        return create(name, null, inputTableNames, outputTableNames);
    }

    public static String create(String name, String snapshot, List<String> inputTableNames, List<String> outputTableNames) {
        BinlogSyncHint migrateHint = new BinlogSyncHint();
        migrateHint.setName(name);
        migrateHint.setInputTableNames(inputTableNames);
        migrateHint.setOutputTableNames(outputTableNames);
        migrateHint.setSnapshotId(snapshot);
        return migrateHint.build();
    }

    @Override
    public String getCmd() {
        return "BINLOG_SYNC";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(this));
    }
}