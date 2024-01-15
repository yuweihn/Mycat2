package io.mycat.ui;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLType;
import io.mycat.ui.chart.DatabaseInstanceChart;
import io.mycat.ui.chart.InstanceChart;
import io.mycat.ui.chart.ReplicaInstanceChart;
import io.mycat.util.SqlTypeUtil;
import io.mycat.util.StringUtil;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@Data
public class MainPaneVO {
    public AnchorPane mainPane;
    public MenuBar menu;
    public TabPane tabPane;
    public HBox runMenu;
    public TextArea inputSql;
    public TableView explain;
    public TableView output;
    public Label statusMessage;
    public Button flashRootButton;
    public Button runButton;
    public Map<String, Controller> tabObjectMap = new HashMap<>();

    public void init() {

        Menu fileMenu = new Menu("文件");
        fileMenu.setId("file");
        MenuItem newConnection = new MenuItem("新连接");
        newConnection.setId("newTCPConnection");
        newConnection.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    final Stage dialog = new Stage();

                    FXMLLoader loader = UIMain.loader("/newConnection.fxml");
                    Parent parent = loader.load();

                    NewConnectionVO newConnectionVO = loader.getController();

                    Scene dialogScene = SceneUtil.createScene(parent, 600, 500);
                    dialog.setScene(dialogScene);
                    dialog.setTitle("新连接");
                    newConnectionVO.getConnect().setId("connect");
                    newConnectionVO.getConnect().setOnAction(new EventHandler<ActionEvent>() {
                        @Override
                        public void handle(ActionEvent event) {

                            try {
                                String name = CheckUtil.isEmpty(newConnectionVO.getNewConnectionName().getText(), "name 不能为空");
                                String url = CheckUtil.isEmpty(newConnectionVO.getUrl().getText(), "url 不能为空");
                                String user = CheckUtil.isEmpty(newConnectionVO.getUser().getText(), "user 不能为空");
                                String password = CheckUtil.isEmpty(newConnectionVO.getPassword().getText(), "password 不能为空");
                                HashMap<String, String> map = new HashMap<>();
                                map.put("url", url);
                                map.put("user", user);
                                map.put("password", password);

                                FXMLLoader loader = UIMain.loader("/mainpane.fxml");
                                Parent parent = loader.load();

                                Controller controller = loader.getController();
                                controller.setInfoProvider(UIMain.getInfoProviderFactory().create(InfoProviderType.TCP, map));
                                controller.flashRoot();
                                controller.getMain().prefWidthProperty().bind(tabPane.widthProperty());//菜单自适应
                                controller.getMain().prefHeightProperty().bind(tabPane.heightProperty());//菜单自适应

                                Tab tab = new Tab(name, parent);
                                tabObjectMap.put(name, controller);
                                tabPane.getTabs().add(tab);
                                SingleSelectionModel selectionModel = tabPane.getSelectionModel();
                                selectionModel.select(tab);
                                dialog.close();
                            } catch (Exception e) {
                                popAlter(e);
                            }

                        }
                    });
                    dialog.showAndWait();
                    SceneUtil.close(dialogScene);
                } catch (Exception exception) {
                    MainPaneVO.popAlter(exception);
                }
            }
        });
        MenuItem newTestConnection = new MenuItem("本地连接");
        newTestConnection.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    final Stage dialog = new Stage();


                    FXMLLoader loader = UIMain.loader("/localConnection.fxml");
                    Parent parent = loader.load();

                    LocalConnectionVO newConnectionVO = loader.getController();

                    Scene dialogScene = SceneUtil.createScene(parent, 600, 500);
                    dialog.setScene(dialogScene);
                    dialog.setTitle("本地连接");
                    newConnectionVO.getConnect().setId("connect");
                    newConnectionVO.getConnect().setOnAction(new EventHandler<ActionEvent>() {
                        @Override
                        public void handle(ActionEvent event) {
                            try {
                                String name = CheckUtil.isEmpty(newConnectionVO.getLocalConnectionName().getText(), "name 不能为空");
//                                String filePath = CheckUtil.isEmpty(newConnectionVO.getFilePath().getText(), "filePath不能为空");
                                HashMap<String, String> map = new HashMap<>();
//                                map.put("filePath", filePath);

                                FXMLLoader loader = UIMain.loader("/mainpane.fxml");
                                Parent parent = loader.load();

                                Controller controller = loader.getController();
                                controller.setInfoProvider(UIMain.getInfoProviderFactory().create(InfoProviderType.LOCAL, map));
                                controller.flashRoot();
                                controller.getMain().prefWidthProperty().bind(tabPane.widthProperty());//菜单自适应
                                controller.getMain().prefHeightProperty().bind(tabPane.heightProperty());//菜单自适应
                                tabObjectMap.put(name, controller);
                                Tab tab = new Tab(name, parent);
                                tab.setOnClosed(new EventHandler<Event>() {
                                    @Override
                                    public void handle(Event event) {
                                        controller.getInfoProvider().close();
                                    }
                                });
                                tabPane.getTabs().add(tab);
                                SingleSelectionModel selectionModel = tabPane.getSelectionModel();
                                selectionModel.select(tab);
                                dialog.close();
                            } catch (Exception e) {
                                popAlter(e);
                            }

                        }
                    });
                    dialog.showAndWait();
                    SceneUtil.close(dialogScene);
                } catch (Exception exception) {
                    MainPaneVO.popAlter(exception);
                }
            }
        });
        fileMenu.getItems().addAll(newConnection, newTestConnection);

        //////////////////////////////////////monitor///////////////////////////////////////////////////////////////////
        MenuItem newMonitorConnection = new MenuItem("监控页");
        newMonitorConnection.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    FXMLLoader loader = UIMain.loader("/monitor.fxml");
                    Parent parent = loader.load();

                    MonitorConnector monitorConnector = loader.getController();

                    EventHandler<ActionEvent> replicaHandler = getMonitorConnectorButtonHandler(monitorConnector,
                            (name, monitorService) -> new ReplicaInstanceChart(name, monitorService));

                    monitorConnector.getReplicaMonitorButton().setOnAction(replicaHandler);

                    EventHandler<ActionEvent> dbHandler = getMonitorConnectorButtonHandler(monitorConnector,
                            (name, monitorService) -> new DatabaseInstanceChart(name, monitorService));
                    monitorConnector.getDbMonitorButton().setOnAction(dbHandler);

                    EventHandler<ActionEvent> instanceHandler = getMonitorConnectorButtonHandler(monitorConnector,
                            (name, monitorService) -> new InstanceChart(name, monitorService));
                    monitorConnector.getInstanceMonitorButton().setOnAction(instanceHandler);

                    Stage stage = new Stage();
                    stage.setTitle("监控页");
                    stage.setScene(new Scene(parent, 600, 400));
                    stage.show();
//                    monitor.setInfoProvider(UIMain.getInfoProviderFactory().create(InfoProviderType.LOCAL, map));
//                    monitorConnector.flashRoot();
//                    monitorConnector.getMain().prefWidthProperty().bind(tabPane.widthProperty());//菜单自适应
//                    monitorConnector.getMain().prefHeightProperty().bind(tabPane.heightProperty());//菜单自适应
//                    tabObjectMap.put(name, monitorConnector);
//                    Tab tab = new Tab(name, parent);
//                    tab.setOnClosed(new EventHandler<Event>() {
//                        @Override
//                        public void handle(Event event) {
//                            monitorConnector.getInfoProvider().close();
//                        }
//                    });
//                    tabPane.getTabs().add(tab);
//                    SingleSelectionModel selectionModel = tabPane.getSelectionModel();
//                    selectionModel.select(tab);
//                    dialog.close();
                } catch (Exception e) {
                    popAlter(e);
                }
            }

            @NotNull
            private EventHandler<ActionEvent> getMonitorConnectorButtonHandler(MonitorConnector monitorConnector, BiFunction<String,MonitorService, Application> factory) {
                return event12 -> {
                    String ipText = monitorConnector.getMonitorIp().getText();
                    String portText = monitorConnector.getMonitorPort().getText();
                    String name = monitorConnector.getMonitorName().getText();
                    if (StringUtil.isEmpty(name)) {
                        Objects.requireNonNull(null, "name  must not be null");
                    }
                    if (StringUtil.isEmpty(ipText)) {
                        Objects.requireNonNull(null, "ip  must not be null");
                    }
                    if (StringUtil.isEmpty(portText)) {
                        Objects.requireNonNull(null, "port  must not be null");
                    }
                    int port = Integer.parseInt(portText);
                    MonitorService monitorService = new MonitorService(ipText, port);
                    try {
                        Stage stage = new Stage();
                        factory.apply(name,monitorService).start(stage);
                    } catch (Exception e) {
                        popAlter(e);
                    }
                };
            }
        });
        fileMenu.getItems().add(newMonitorConnection);

        Menu helpMenu = new Menu("帮助");
        helpMenu.setId("help");
        MenuItem aboutMenu = new MenuItem("关于");
        aboutMenu.setId("about");
        aboutMenu.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                final Stage dialog = new Stage();
                dialog.initModality(Modality.APPLICATION_MODAL);
                VBox dialogVbox = new VBox(20);
                dialogVbox.getChildren().add(new TextField("https://github.com/MyCATApache/Mycat2 "));
                Button button = new Button("关闭");
                button.setId("closeAbout");
                button.setOnAction(event1 -> dialog.close());
                dialogVbox.getChildren().add(button);
                Scene dialogScene = SceneUtil.createScene(dialogVbox, 300, 200);
                dialog.setScene(dialogScene);
                dialog.setTitle("关于");
                dialog.showAndWait();
                SceneUtil.close(dialogScene);
            }
        });
        helpMenu.getItems().addAll(aboutMenu);

        menu.getMenus().addAll(fileMenu, helpMenu);

        AtomicReference<Tab> selectTab = new AtomicReference<>();
        tabPane.getSelectionModel().selectedItemProperty().addListener((obs, ov, nv) -> {
            selectTab.set(nv);
        });

        runButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {

                TableViewOuter outputText = new TableViewOuter(output);
                TableViewOuter explainText = new TableViewOuter(explain);

                Tab s = selectTab.get();

                if (s != null) {
                    String text = s.getText();
                    Controller controller = tabObjectMap.get(text);
                    InfoProvider infoProvider = controller.getInfoProvider();
                    String sql = inputSql.getText();
                    List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, DbType.mysql);
                    Connection connection = infoProvider.createConnection();
                    for (SQLStatement sqlStatement : sqlStatements) {
                        SQLType sqlType = SQLParserUtils.getSQLTypeV2(sqlStatement.toString(), DbType.mysql);
                        boolean select = !SqlTypeUtil.isDml(sqlType);
                        try (Statement statement = connection.createStatement();) {
                            if (select) {
                                ResultSet resultSet = statement.executeQuery(sql);
                                TableData tableData = ResultSetPrinter.getTable(resultSet);
                                outputText.appendData(tableData);
                            } else {
                                boolean affectRow = statement.execute(sqlStatement.toString());
                                outputText.setPlaceholder("affectRow:" + affectRow);
                            }
                            MySqlExplainStatement mySqlExplainStatement = new MySqlExplainStatement();
                            mySqlExplainStatement.setStatement(sqlStatement.clone());
                            ResultSet resultSet = statement.executeQuery(mySqlExplainStatement.toString());
                            TableData tableData = ResultSetPrinter.getTable(resultSet);
                            explainText.appendData(tableData);
                        } catch (Exception e) {
                            outputText.setPlaceholder(e.getLocalizedMessage());
                            MainPaneVO.popAlter(e);
                        }
                    }

                }
            }
        });
        flashRootButton.setOnAction(event -> {
            Optional.ofNullable(selectTab.get()).map(tab -> tab.getText()).map(text -> {
                return tabObjectMap.get(text);
            }).ifPresent(controller -> {
                controller.flashSchemas();
            });
        });

    }

    public static void popAlter(Exception e) {
        e.printStackTrace();
        Stage stage = new Stage();
        stage.initModality(Modality.APPLICATION_MODAL);
        stage.setTitle("警告");
        VBox pane = new VBox();

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        e.printStackTrace(printWriter);
        pane.getChildren().add(new TextArea(result.toString()));

        Button button = new Button();
        button.setText("关闭");
        button.setOnAction(event -> stage.close());
        pane.getChildren().add(button);
        Scene scene = new Scene(pane);
        stage.setScene(scene);
        stage.showAndWait();
        stage.close();
    }
}
