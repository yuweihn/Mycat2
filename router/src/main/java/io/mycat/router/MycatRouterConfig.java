/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router;

import io.mycat.MycatException;
import io.mycat.api.MySQLAPIRuntime;
import io.mycat.beans.mycat.DefaultTable;
import io.mycat.beans.mycat.ERTable;
import io.mycat.beans.mycat.GlobalTable;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mycat.MycatTableRule;
import io.mycat.beans.mycat.ShardingDatabseTableTable;
import io.mycat.beans.mycat.ShardingDbTable;
import io.mycat.beans.mycat.ShardingTableTable;
import io.mycat.config.ConfigFile;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.YamlUtil;
import io.mycat.config.route.AnnotationType;
import io.mycat.config.route.DynamicAnnotationConfig;
import io.mycat.config.route.DynamicAnnotationRootConfig;
import io.mycat.config.route.SchemaSequenceModifierRootConfig;
import io.mycat.config.route.SequenceModifierConfig;
import io.mycat.config.route.ShardingFuntion;
import io.mycat.config.route.ShardingRule;
import io.mycat.config.route.ShardingRuleRootConfig;
import io.mycat.config.route.SharingFuntionRootConfig;
import io.mycat.config.route.SharingTableRule;
import io.mycat.config.route.SubShardingFuntion;
import io.mycat.config.schema.SchemaConfig;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.config.schema.SchemaType;
import io.mycat.config.schema.TableDefConfig;
import io.mycat.router.dynamicAnnotation.DynamicAnnotationMatcherImpl;
import io.mycat.router.routeStrategy.AnnotationRouteStrategy;
import io.mycat.router.routeStrategy.DbInMultiServerRouteStrategy;
import io.mycat.router.routeStrategy.DbInOneServerRouteStrategy;
import io.mycat.router.routeStrategy.SqlParseRouteRouteStrategy;
import io.mycat.sequenceModifier.SequenceModifier;
import io.mycat.util.SplitUtil;
import io.mycat.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author jamie12221 date 2019-05-03 00:29
 **/
public class MycatRouterConfig {

  private final Map<String, Supplier<RuleAlgorithm>> functions = new HashMap<>();
  private final Map<String, DynamicAnnotationConfig> dynamicAnnotations = new HashMap<>();
  private final Map<String, MycatTableRule> tableRules = new HashMap<>();
  private final Map<String, MycatSchema> schemas = new HashMap<>();
  private final Map<String, SequenceModifier> sequenceModifiers = new HashMap<>();
  private final MycatSchema defaultSchema;
  private ConfigReceiver cr;
  private MySQLAPIRuntime mySQLAPIRuntime;

  public MycatTableRule getTableRuleByTableName(String name) {
    return tableRules.get(name);
  }

  public void putDynamicAnnotation(String name, DynamicAnnotationConfig dynamicAnnotation) {
    dynamicAnnotations.put(name, dynamicAnnotation);
  }

  public DynamicAnnotationMatcherImpl getDynamicAnnotationMatcher(List<String> names) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String name : names) {
      list.add(dynamicAnnotations.get(name));
    }
    return new DynamicAnnotationMatcherImpl(list);
  }

  /**/
  private static void init() throws CloneNotSupportedException {
    SchemaRootConfig schemaRootConfig = new SchemaRootConfig();

    SchemaConfig sc = new SchemaConfig();
    schemaRootConfig.setSchemas(Arrays.asList(sc));
    sc.setDefaultDataNode("dafault");
    sc.setName("schemaName");
    sc.setSchemaType(SchemaType.DB_IN_ONE_SERVER);
    sc.setSqlMaxLimit("100");

    TableDefConfig table = new TableDefConfig();

    sc.setTables(Arrays.asList(table));

    table.setTableRule("rule1");
    table.setDataNodes("dn1,dn2");

    ShardingRuleRootConfig src = new ShardingRuleRootConfig();

    SharingTableRule sharingTableRule = new SharingTableRule();
    src.setTableRules(Collections.singletonList(sharingTableRule));

    ShardingRule s1 = new ShardingRule();
    ShardingRule s2 = new ShardingRule();

    sharingTableRule.setRules(Arrays.asList(s1, s2));

    sharingTableRule.setFunction("mpartitionByLong");

    s1.setColumn("id1");
    s2.setColumn("id2");

    s1.setEqualAnnotations(Arrays.asList("(?:id = )(?<id1>([0-9]))"));
    s2.setEqualAnnotations(Arrays.asList("(?:id = )(?<id2>([0-9]))"));
    s1.setEqualKey("id1");
    s2.setEqualKey("id2");

    s1.setRangeAnnotations(
        Arrays.asList("(?<between>((?:between )(?<id1s>[0-9])(?: and )(?<id1e>[0-9])))"));
    s2.setRangeAnnotations(
        Arrays.asList("(?<between>((?:between )(?<id2s>[0-9])(?: and )(?<id2e>[0-9])))"));

    s1.setRangeStartKey("id1s");
    s1.setRangeEndKey("id1e");

    s2.setRangeStartKey("id2s");
    s2.setRangeEndKey("id2e");

    SharingFuntionRootConfig sfrc = new SharingFuntionRootConfig();
    ShardingFuntion shardingFuntion = new ShardingFuntion();
    sfrc.setFunctions(Arrays.asList(shardingFuntion));
    shardingFuntion.setName("partitionByLong");
    shardingFuntion.setClazz("io.mycat.router.function.PartitionByLong");
    Map<String, String> properties = new HashMap<>();
    shardingFuntion.setProperties(properties);
    properties.put("partitionCount", "8");
    properties.put("partitionLength", "128");

    SubShardingFuntion subShardingFuntion = new SubShardingFuntion();
    subShardingFuntion.setClazz("io.mycat.router.function.PartitionByLong");
    Map<String, String> subProperties = new HashMap<>();
    subShardingFuntion.setProperties(subProperties);
    subProperties.put("partitionCount", "8");
    subProperties.put("partitionLength", "128");
    shardingFuntion.setSubFuntion(subShardingFuntion);

    String dump = YamlUtil.dump(src);
  }

  public static List<DynamicAnnotationConfig> getDynamicAnnotationConfigList(List<String> patterns,
      AnnotationType type) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String pattern : patterns) {
      DynamicAnnotationConfig dynamicAnnotationConfig = new DynamicAnnotationConfig();
      dynamicAnnotationConfig.setType(type);
      dynamicAnnotationConfig.setPattern(pattern);
      list.add(dynamicAnnotationConfig);
    }
    return list;
  }

  public void putRuleAlgorithm(ShardingFuntion funtion) {

    functions.put(funtion.getName(), () -> {
      try {
        String name = funtion.getName();
        RuleAlgorithm rootFunction = getRuleAlgorithm(funtion);
        ShardingFuntion rootConfig = funtion;
        SubShardingFuntion subFuntionConfig = rootConfig.getSubFuntion();
        if (subFuntionConfig != null) {
          rootFunction.setSubRuleAlgorithm(getSubRuleAlgorithmList(
              rootFunction.getPartitionNum(),
              name,
              subFuntionConfig));
        }
        return rootFunction;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static RuleAlgorithm createFunction(String name, String clazz)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    Class<?> clz = Class.forName(clazz);
    //判断是否继承AbstractPartitionAlgorithm
    if (!RuleAlgorithm.class.isAssignableFrom(clz)) {
      throw new IllegalArgumentException("rule function must implements "
          + RuleAlgorithm.class.getName() + ", name=" + name);
    }
    return (RuleAlgorithm) clz.newInstance();
  }

  public MycatRouterConfig(ConfigReceiver cr, MySQLAPIRuntime runtime) {
    this(cr.getConfig(ConfigFile.SCHEMA)
        , cr.getConfig(ConfigFile.FUNCTIONS),
        cr.getConfig(ConfigFile.DYNAMIC_ANNOTATION),
        cr.getConfig(ConfigFile.SEQUENCE_MODIFIER),
        cr.getConfig(ConfigFile.RULE), runtime);
  }

  private RuleAlgorithm getRuleAlgorithm(ShardingFuntion funtion)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Map<String, String> properties = funtion.getProperties();
    properties = (properties == null) ? Collections.emptyMap() : properties;
    funtion.setProperties(properties);
    RuleAlgorithm rootFunction = createFunction(funtion.getName(), funtion.getClazz());
    rootFunction.init(funtion.getProperties(), funtion.getRanges());
    return rootFunction;
  }

  public MycatRouterConfig(SchemaRootConfig schemaConfig,
      SharingFuntionRootConfig funtionsConfig,
      DynamicAnnotationRootConfig dynamicAnnotationConfig,
      SchemaSequenceModifierRootConfig sequenceModifierRootConfig,
      ShardingRuleRootConfig ruleConfig, MySQLAPIRuntime mySQLAPIRuntime) {
    this.mySQLAPIRuntime = mySQLAPIRuntime;
    ////////////////////////////////////check/////////////////////////////////////////////////
//    Objects.requireNonNull(schemaConfig, "schema config can not be empty");
//    Objects.requireNonNull(funtionsConfig, "function config can not be empty");
//    Objects.requireNonNull(dynamicAnnotationConfig, "dynamicAnnotation config can not be empty");
//    Objects.requireNonNull(ruleConfig, "rule config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    initFunctions(funtionsConfig);
    initAnnotations(dynamicAnnotationConfig);
    initModifiers(sequenceModifierRootConfig);
    initTableRule(ruleConfig);
    iniSchema(schemaConfig);
    this.defaultSchema = initDefaultSchema(schemaConfig);
    this.mySQLAPIRuntime = mySQLAPIRuntime;
  }

  public RuleAlgorithm getRuleAlgorithm(String name) {
    return functions.get(name).get();
  }

  private static SequenceModifier createSequenceModifier(String clazz)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    Class<?> clz = Class.forName(clazz);
    //判断是否继承AbstractPartitionAlgorithm
    if (!SequenceModifier.class.isAssignableFrom(clz)) {
      throw new IllegalArgumentException("SequenceModifierConfig must implements "
          + SequenceModifier.class.getName() + ", clazz=" + clazz);
    }
    return (SequenceModifier) clz.newInstance();
  }

  private SequenceModifier getSequenceModifier(MySQLAPIRuntime mySQLAPIRuntime,
      String sequenceClass, Map<String, String> properties) {
    if (sequenceClass != null) {
      try {
        SequenceModifier sequenceModifier = createSequenceModifier(sequenceClass);
        properties = (properties == null) ? Collections.emptyMap() : properties;
        sequenceModifier.init(mySQLAPIRuntime, properties);
        return sequenceModifier;
      } catch (Exception e) {
        throw new MycatException("can not init {}", sequenceClass, e);
      }
    }
    return null;
  }

  private void initModifiers(SchemaSequenceModifierRootConfig config) {
    if (config != null) {
      Map<String, SequenceModifierConfig> modifiers = config
          .getModifiers();
      if (modifiers != null) {
        for (Entry<String, SequenceModifierConfig> entry : modifiers
            .entrySet()) {
          String key = entry.getKey();
          SequenceModifierConfig value = entry.getValue();
          SequenceModifier sequenceModifier = getSequenceModifier(this.mySQLAPIRuntime,
              value.getSequenceModifierClazz(),
              value.getSequenceModifierProperties());
          this.sequenceModifiers.put(key, sequenceModifier);
        }
      }
    }
  }

  public MycatSchema getDefaultSchema() {
    return defaultSchema;
  }

  private MycatSchema initDefaultSchema(SchemaRootConfig schemaConfigs) {
    String defaultSchemaName = schemaConfigs.getDefaultSchemaName();
    if (defaultSchemaName == null || "".equals(defaultSchemaName)) {
      defaultSchemaName = schemaConfigs.getSchemas().get(0).getName();
    }
    return schemas.get(defaultSchemaName);
  }

  private List<RuleAlgorithm> getSubRuleAlgorithmList(int partitionNum, String parent,
      SubShardingFuntion funtion)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Map<String, String> properties = funtion.getProperties();
    Map<String, String> ranges = funtion.getRanges();
    List<RuleAlgorithm> ruleAlgorithms = new ArrayList<>();
    if (properties == null) {
      properties = Collections.EMPTY_MAP;
    }
    if (ranges == null) {
      ranges = Collections.EMPTY_MAP;
    }
    for (int i = 0; i < partitionNum; i++) {
      RuleAlgorithm function = createFunction(parent + funtion.toString(), funtion.getClazz());
      function.init(properties, ranges);
      ruleAlgorithms.add(function);
      if (funtion.getSubFuntion() != null) {
        function.setSubRuleAlgorithm(
            getSubRuleAlgorithmList(function.getPartitionNum(), parent, funtion.getSubFuntion()));
      }
    }
    return ruleAlgorithms;
  }

  private void initAnnotations(DynamicAnnotationRootConfig config) {
    MycatRouterConfig mycatRouter = this;
    if (config != null && config.getDynamicAnnotations() != null) {
      List<DynamicAnnotationConfig> annotations = config.getDynamicAnnotations();
      for (DynamicAnnotationConfig a : annotations) {
        ////////////////////////////////////check/////////////////////////////////////////////////
        Objects.requireNonNull(a.getName(), "name of dynamicAnnotation can not be empty");
        Objects.requireNonNull(a.getType(), "type of dynamicAnnotation can not be empty");
        Objects.requireNonNull(a.getPattern(), "pattern of dynamicAnnotation can not be empty");
        Objects.requireNonNull(a.getGroupNameList(),
            "group name of dynamicAnnotation can not be empty");
        ////////////////////////////////////check/////////////////////////////////////////////////
        mycatRouter.putDynamicAnnotation(a.getName(), a);
      }
    }
  }

  private void initFunctions(SharingFuntionRootConfig funtions) {
    MycatRouterConfig mycatRouter = this;
    if (funtions != null) {
      if (funtions.getFunctions() != null) {
        for (ShardingFuntion funtion : funtions.getFunctions()) {
          ////////////////////////////////////check/////////////////////////////////////////////////
          Objects.requireNonNull(funtion.getName(), "name of function can not be empty");
          Objects.requireNonNull(funtion.getClazz(), "clazz of function can not be empty");
          ////////////////////////////////////check/////////////////////////////////////////////////
          mycatRouter.putRuleAlgorithm(funtion);
        }
      }
    }
  }

//
//    String sql = "select * from travelrecord where  id = 1 and id2 between 2 and 3;";
//
//    DynamicAnnotationResultImpl result = matcher.match(sql);
//
//    List<MycatTable> tables = new ArrayList<>();
//    RouteContext sqlContext = new RouteContext();
//    sqlContext.setResult(result);
//    rootRouteNode.enterRoute(result, algorithm, sqlContext);

  public DynamicAnnotationMatcherImpl getDynamicAnnotationMatcherAndResetType(List<String> names,
      AnnotationType type) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String name : names) {
      DynamicAnnotationConfig dynamicAnnotationConfig = dynamicAnnotations.get(name);
      dynamicAnnotationConfig = (DynamicAnnotationConfig) dynamicAnnotationConfig.clone();
      dynamicAnnotationConfig.setType(type);// nullexception
      list.add(dynamicAnnotationConfig);
    }
    return new DynamicAnnotationMatcherImpl(list);
  }

  private void iniSchema(SchemaRootConfig schemaConfigs) {
    Objects.requireNonNull(schemaConfigs);
    for (SchemaConfig schemaConfig : schemaConfigs.getSchemas()) {
      String defaultDataNode = schemaConfig.getDefaultDataNode();
      String sqlMaxLimit = schemaConfig.getSqlMaxLimit();
      SchemaType schemaType = schemaConfig.getSchemaType();
      RouteStrategy routeStrategy = null;
      switch (schemaType) {
        case DB_IN_ONE_SERVER:
          routeStrategy = new DbInOneServerRouteStrategy();
          break;
        case DB_IN_MULTI_SERVER:
          routeStrategy = new DbInMultiServerRouteStrategy();
          break;
        case ANNOTATION_ROUTE:
          routeStrategy = new AnnotationRouteStrategy();
          break;
        case SQL_PARSE_ROUTE:
          routeStrategy = new SqlParseRouteRouteStrategy();
          break;
      }
      MycatSchema schema = new MycatSchema(schemaConfig, routeStrategy,
          this.sequenceModifiers.get(schemaConfig.getName()));
      if (sqlMaxLimit != null && !"".equals(sqlMaxLimit)) {
        schema.setSqlMaxLimit(Long.parseLong(sqlMaxLimit));
      }
      if (defaultDataNode != null && !"".equals(defaultDataNode)) {
        schema.setDefaultDataNode(defaultDataNode);
      }
      List<TableDefConfig> tables = schemaConfig.getTables();
      if (tables != null) {
        Map<String, MycatTable> mycatTables = new HashMap<>();
        for (TableDefConfig tableConfig : tables) {
          String dataNodeText = tableConfig.getDataNodes();
          String tableName = tableConfig.getName();

          String tableRuleName = tableConfig.getTableRule();
          MycatTableRule tableRule = getTableRuleByTableName(tableName);
          List<String> dataNodes = Collections.EMPTY_LIST;

          if (!StringUtil.isEmpty(dataNodeText)) {
            dataNodes = Arrays.asList(SplitUtil.split(dataNodeText, ","));
          }
          MycatTable table;

          if (tableConfig.getType() != null) {
            switch (tableConfig.getType()) {
              case GLOBAL:
                mycatTables.put(tableName, table = new GlobalTable(schema, tableConfig, dataNodes));
                break;
              case SHARING_DATABASE:
                mycatTables
                    .put(tableName,
                        table = new ShardingDbTable(schema, tableConfig, dataNodes, tableRule));
                break;
              case SHARING_TABLE:
                mycatTables.put(tableName,
                    table = new ShardingTableTable(schema, tableConfig, dataNodes, tableRule));
                break;
              case SHARING_DATABASE_TABLE:
                mycatTables
                    .put(tableName,
                        table = new ShardingDatabseTableTable(schema, tableConfig, dataNodes,
                            tableRule));
                break;
              case ER:
                mycatTables
                    .put(tableName, table = new ERTable(schema, tableConfig, dataNodes, tableRule));
                break;
              default:
                throw new MycatException("");
            }
          } else {
            mycatTables
                .put(tableName,
                    table = new DefaultTable(schema, tableConfig, dataNodes));
          }

        }
        schema.setTables(mycatTables);

        schemas.put(schemaConfig.getName(), schema);
      }
    }
  }

  private void initTableRule(ShardingRuleRootConfig rule) {
    MycatRouterConfig mycatRouter = this;
    if (rule == null) {
      return;
    }
    if (rule.getTableRules() != null) {
      for (SharingTableRule tableRule : rule.getTableRules()) {
        String name = tableRule.getTableName();
        ////////////////////////////////////check/////////////////////////////////////////////////
        Objects.requireNonNull(name, "name of table can not be empty");
        Objects.requireNonNull(tableRule.getRules(), "rule of table can not be empty");
        ////////////////////////////////////check/////////////////////////////////////////////////
        Route rootRouteNode = null;
        Route routeNode = null;
        List<ShardingRule> rules = tableRule.getRules();
        List<DynamicAnnotationConfig> list = new ArrayList<>();
        RuleAlgorithm algorithm = null;
        for (ShardingRule shardingRule : rules) {
          String column = shardingRule.getColumn();
          Set<String> equalsKey = Collections.emptySet();
          Set<String> rangeStartKey = Collections.emptySet();
          Set<String> rangeEndKey = Collections.emptySet();
          ////////////////////////////////////check/////////////////////////////////////////////////
          Objects.requireNonNull(column, "column of table can not be empty");
          ////////////////////////////////////check/////////////////////////////////////////////////
          List<String> equal = shardingRule.getEqualAnnotations();
          if (equal != null) {
            list.addAll(getDynamicAnnotationConfigList(equal, AnnotationType.SHARDING_EQUAL));
            ////////////////////////////////////check/////////////////////////////////////////////////
            Objects.requireNonNull(shardingRule.getEqualKeys(),
                "equal key of table can not be empty in equal annotations");
            ////////////////////////////////////check/////////////////////////////////////////////////
            equalsKey = new HashSet<>(
                Arrays.asList(SplitUtil.split(shardingRule.getEqualKeys(), ",")));
          }
          List<String> range = shardingRule.getRangeAnnotations();
          if (range != null) {
            list.addAll(getDynamicAnnotationConfigList(range, AnnotationType.SHARDING_RANGE));
            ////////////////////////////////////check/////////////////////////////////////////////////
            Objects.requireNonNull(shardingRule.getRangeStartKey(),
                "start key of table can not be empty in range annotations");
            Objects.requireNonNull(shardingRule.getRangeEndKey(),
                "end key of table can not be empty in range annotations");
            ////////////////////////////////////check/////////////////////////////////////////////////
            rangeStartKey = new HashSet<>(
                Arrays.asList(SplitUtil.split(shardingRule.getRangeStartKey(), ",")));
            rangeEndKey = new HashSet<>(
                Arrays.asList(SplitUtil.split(shardingRule.getRangeEndKey(), ",")));
          }

          Route tmp = new Route(column, equalsKey, rangeStartKey, rangeEndKey);
          if (rootRouteNode == null) {
            String funtion = tableRule.getFunction();
            algorithm = mycatRouter.getRuleAlgorithm(funtion);
            routeNode = rootRouteNode = tmp;
          } else {
            routeNode.setNextRoute(tmp);
            routeNode = tmp;
          }
        }
        DynamicAnnotationMatcherImpl matcher;
        if (!list.isEmpty()) {
          matcher = new DynamicAnnotationMatcherImpl(list);
        } else {
          matcher = DynamicAnnotationMatcherImpl.EMPTY;
        }
        tableRules.put(name,
            new MycatTableRule(name,
                getSequenceModifier(this.mySQLAPIRuntime, tableRule.getSequenceClass(),
                    tableRule.getSequenceProperties()), rootRouteNode, algorithm,
                matcher));
      }
    }

  }

//  private void initDataNode(DataNodeRootConfig dataNode) {
//    MycatRouterConfig mycatRouter = this;
//
//    for (DataNodeConfig dataNodeConfig : dataNode.getDataNodes()) {
//      DataNodeType dataNodeType =
//          dataNodeConfig.getType() == null ? DataNodeType.MYSQL : dataNodeConfig.getType();
//      switch (dataNodeType) {
//        case MYSQL:
//          MySQLDataNode mySQLDataNode = new MySQLDataNode(dataNodeConfig);
//          dataNodes.put(dataNodeConfig.getTableName(), mySQLDataNode);
//          break;
//      }
//    }
//  }

//  private void initSQLinterceptor(ShardingRuleRootConfig rule) {
//    String sqlInterceptorClass = rule.getSqlInterceptorClass();
//    if (!(StringUtil.isEmpty(sqlInterceptorClass))) {
//      try {
//        Class<?> clz = Class.forName(sqlInterceptorClass);
//        sqlInterceptor = (SQLInterceptor) clz.newInstance();
//      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
//        e.printStackTrace();
//      }
//    } else {
//      sqlInterceptor = sql -> sql;
//    }
//  }

  public MycatSchema getSchemaBySchemaName(String name) {
    return schemas.get(name);
  }

  public MycatSchema getSchemaOrDefaultBySchemaName(String name) {
    MycatSchema schema = getSchemaBySchemaName(name);
    if (schema == null) {
      return defaultSchema;
    } else {
      return schema;
    }
  }

  public Collection<MycatSchema> getSchemaList() {
    return schemas.values();
  }

//  public SQLInterceptor getSqlInterceptor() {
//    return sqlInterceptor;
//  }
}
