package io.mycat.client;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.booster.CacheConfig;
import io.mycat.booster.Task;
import io.mycat.commands.MycatCommand;
import io.mycat.lib.impl.CacheFile;
import io.mycat.lib.impl.CacheLib;
import io.mycat.matcher.Matcher;
import io.mycat.plug.command.MycatCommandLoader;
import io.mycat.plug.hint.HintLoader;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SimpleTransactionSessionRunner;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import io.mycat.util.StringUtil;
import lombok.Getter;
import org.apache.calcite.util.Template;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Junwen Chen
 **/
@Getter
public class UserSpace {
    private static final Logger logger = LoggerFactory.getLogger(UserSpace.class);
    private final String userName;
    private final TransactionType defaultTransactionType;
    private final Matcher<Map<String, Object>> matcher;
    private final Map<String, Task> cacheMap = new ConcurrentHashMap<>();

    public UserSpace(String userName, TransactionType defaultTransactionType, Matcher matcher, List<CacheTask> cacheTaskList) {
        this.userName = Objects.requireNonNull(userName);
        this.defaultTransactionType = Objects.requireNonNull(defaultTransactionType);
        this.matcher = matcher;
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        cacheTaskList.forEach(cacheTask -> {
            MycatDataContext context = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
            MycatDBClientMediator db = MycatDBs.createClient(context);
            Task task = getTask(cacheTask.getText(), cacheTask.getType(), db, timer, cacheTask.getCacheConfig());
            cacheMap.put(cacheTask.getName(), task);
            task.start();
        });
    }

    public MycatDBClientMediator create(MycatDataContext dataContext) {
        return MycatDBs.createClient(dataContext);
    }


    public void execute(final ByteBuffer buffer, final MycatSession session, Response response) {
        final CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
        final Map<String, Object> extractor = new HashMap<>();
        List<Map<String, Object>> matchList = matcher.match(charBuffer, extractor);
        if (matchList == null) {
            matchList = Collections.emptyList();
        }
        MycatDataContext dataContext = session.getDataContext();
        int sessionId = session.sessionId();
        for (Map<String, Object> item : matchList) {
            HashMap<String, Object> context = new HashMap<>(item);
            context.putAll(extractor);
            if (execute(sessionId, dataContext, charBuffer, context, response)) return;
        }
        response.sendError(new MycatException("No matching commands"));
    }

    public boolean execute(int sessionId, MycatDataContext dataContext, CharBuffer charBuffer, Map<String, Object> context, Response response) {
        try {
            final String name = Objects.requireNonNull((String) context.get("name"), "command is not allowed null");
            final String command = Objects.requireNonNull((String) context.get("command"), "command is not allowed null");
            final List<String> hints = (List<String>) context.getOrDefault("hints", Collections.emptyList());
            final boolean explainCommand = "true".equalsIgnoreCase(Objects.toString(context.getOrDefault("doExplain", "")));
            //////////////////////////////////hints/////////////////////////////////
            String text = null;
            if (!hints.isEmpty()) {
                text = charBuffer.toString();
                for (String hintName : hints) {
                    Objects.requireNonNull(HintLoader.INSTANCE.get(hintName)).accept(text, context);
                }
            }
            //////////////////////////////////hints/////////////////////////////////
            final boolean cache = !StringUtil.isEmpty((String) context.get("cache"));
            ///////////////////////////////////cache//////////////////////////////////
            if (cache) {
                Optional<MycatResultSetResponse> mycatResultSetResponse = Optional.ofNullable(cacheMap.get(name)).map(i -> i.get());
                if (mycatResultSetResponse.isPresent()) {
                    logger.info("\n" + context + "\n hit cache");
                    response.sendResponse(new MycatResponse[]{mycatResultSetResponse.get()}, () -> Arrays.asList("cache :" + context));
                    return true;
                }
            }
            ///////////////////////////////////cache//////////////////////////////////
            //////////////////////////////////tags/////////////////////////////////
            final Map<String, Object> tags = (Map<String, Object>) context.getOrDefault("tags", Collections.emptyMap());
            //////////////////////////////////tags/////////////////////////////////
            //////////////////////////////////explain/////////////////////////////////
            String explain = (String) context.get("explain");
            if (StringUtil.isEmpty(explain)) {
                if (text == null) {
                    text = charBuffer.toString();
                }
            } else {
                text = Template.formatByName(explain, (Map) tags);
            }
            if (text != null) {
                text = charBuffer.toString();
            }
            Objects.requireNonNull(text);
            //////////////////////////////////command/////////////////////////////////
            MycatCommand commandHanlder = Objects.requireNonNull(MycatCommandLoader.INSTANCE.get(command));
            MycatRequest sqlRequest = new MycatRequest(sessionId, text, context, this);
            if (explainCommand) {
                return commandHanlder.explain(sqlRequest, dataContext, response);
            } else {
                return commandHanlder.run(sqlRequest, dataContext, response);
            }
            //////////////////////////////////command/////////////////////////////////
        } catch (Throwable e) {
            logger.error("", e);
        }
        return false;
    }

    //解决获取结果集对象查询和更新的顺序问题,不解决正在写入的结果集回收的问题

    @NotNull
    public static Task getTask(final String text, Type type, MycatDBClientMediator db, ScheduledExecutorService timer, CacheConfig cacheConfig) {
        return new Task(cacheConfig) {
            volatile CacheFile cache;

            @Override
            public void start(CacheConfig cacheConfig) {
                NameableExecutor mycatWorker = MycatWorkerProcessor.INSTANCE.getMycatWorker();
                timer.scheduleAtFixedRate(() -> mycatWorker.execute(() -> {
                            try {
                                cache(cacheConfig);
                            } catch (Exception e) {
                                logger.error("build cache fail:"+cacheConfig, e);
                            }
                        }),
                        cacheConfig.getInitialDelay().toMillis(),
                        cacheConfig.getRefreshInterval().toMillis(),
                        TimeUnit.MILLISECONDS);
            }

            @Override
            public synchronized void cache(CacheConfig cacheConfig) {
                RowBaseIterator query = dispatcher(type, text);
                CacheFile cache2 = cache;
                try {
                    String finalText = text;
                    finalText = finalText.replaceAll("[\\?\\\\/:|<>\\*]", " "); //filter ? \ / : | < > *
                    finalText = finalText.replaceAll("\\s+", "_");
                    if (text.length() > 5) {
                        finalText = finalText.substring(0, 5);
                    }
                    cache = CacheLib.cache(() -> new TextResultSetResponse(query), finalText.replaceAll(" ", "_"));
                } finally {
                    if (query !=null){
                        query.close();
                    }
                    if (cache2 != null) {
                        cache2.close();
                    }
                }
            }

            private RowBaseIterator dispatcher(Type command, String text) {
                RowBaseIterator query;
                try {
                    switch (command) {
                        case SQL: {
                            query = db.query(text);
                            break;
                        }
                        case HBT: {
                            query = db.executeRel(text);
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException(command.toString());
                    }
                } catch (Throwable t){
                    logger.error("",t);
                    throw t;
                }finally {
                    db.recycleResource();
                }
                return query;
            }

            @Override
            public MycatResultSetResponse get(CacheConfig cacheConfig) {
                if (cache != null) {
                    return cache.cacheResponse();
                } else return null;
            }
        };
    }


}