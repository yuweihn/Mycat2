/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat;

import com.alibaba.fastjson.JSONObject;
import io.mycat.util.JsonUtil;
import io.mycat.util.YamlUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class FileConfigProvider implements ConfigProvider {
    volatile MycatConfig config;
    private String defaultPath;
    final AtomicInteger count = new AtomicInteger();
   static final Logger logger = LoggerFactory.getLogger(FileConfigProvider.class);

    @Override
    public void init(Map<String, String> config) throws Exception {
        this.defaultPath = config.get("path");
        fetchConfig(this.defaultPath);
    }

    @Override
    public void fetchConfig() throws Exception {
        fetchConfig(defaultPath);
    }

    @Override
    public synchronized void report(MycatConfig changed) {
        try {
            backup();
            YamlUtil.dumpToFile(defaultPath, YamlUtil.dump(changed));
            config = changed;
        }catch (Throwable e){
            logger.error("",e);
        }
    }

    private void backup() {
        try {
            YamlUtil.dumpBackupToFile(defaultPath,count.getAndIncrement(),YamlUtil.dump(config));
        } catch (Exception e) {
            logger.error("",e);
        }
    }

    @Override
    public void fetchConfig(String url) throws Exception {
        Path asbPath = Paths.get(url).toAbsolutePath();
        if (!Files.exists(asbPath)) {
            throw new IllegalArgumentException(MessageFormat.format("path not found: {0}", Objects.toString(asbPath)));
        }
        Iterator<String> iterator = Files.lines(asbPath).iterator();
        StringBuilder sqlGroups  = new StringBuilder();
        StringBuilder full  = new StringBuilder();
        boolean in= false;
        while (iterator.hasNext()){
            String next = iterator.next();
            if (next.startsWith("#lib start")){
                sqlGroups.append(next).append('\n');
                in = true;
            }else if (in){
                sqlGroups.append(next).append('\n');
            }else if (next.startsWith("#lib end")){
                sqlGroups.append(next).append('\n');
                in =false;
            }else {
                full.append(next).append('\n');
            }
        }
        sqlGroups.append(full);
        System.out.println(sqlGroups);
        config = YamlUtil.loadText(sqlGroups.toString(), MycatConfig.class);
    }


    @Override
    public MycatConfig currentConfig() {
        return config;
    }

    @Override
    public synchronized void reportReplica(String replicaName, List<String> dataSourceList) {
        try{
        Path resolve = Paths.get(defaultPath).getParent().resolve("replica.log");
        StringBuilder outputStreamWriter = new StringBuilder();
        outputStreamWriter.append(ReplicaInfo.builder().replicaName(replicaName).dataSourceList(dataSourceList).build());
        outputStreamWriter.append("\n");
        logger.error("switch log: ",outputStreamWriter);
        Files.write(resolve,outputStreamWriter.toString().getBytes(), StandardOpenOption.APPEND,StandardOpenOption.CREATE);
        }catch (Throwable e){
            logger.error("",e);
        }
    }

    @Getter
    @Builder
    @ToString
    static class ReplicaInfo{
        String    replicaName;
        List<String> dataSourceList;
    }
}