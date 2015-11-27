import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.codehaus.jettison.json.JSONArray;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by yudun on 15/10/17.
 */
public class CCTeam extends AbstractVerticle {

    static ConfigSingleton config = null;

    Router router = Router.router(vertx);


    public void home(Route routerHome){
        routerHome.handler(routingContext -> {
            routingContext.response().end("<h1>Hi, We are " + config.TEAMID + "!" +
                    "<br> This is our CC team project home page</h1>");
        });
    }

    public void Q1(Route routerQ1){
        routerQ1.handler(routingContext -> {
            String key = routingContext.request().getParam("key");
            String message = routingContext.request().getParam("message");

            String result = Q1.decrypt(key, message);
            routingContext.response().end(result);

        });
    }

    public void Q2Mysql(SQLConnection connection, Route routerQ2){

        routerQ2.handler(routingContext -> {
            String userid = routingContext.request().getParam("userid");
            String tweet_time = routingContext.request().getParam("tweet_time");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Q2.lookupMysql(connection, userid, tweet_time, routingContext);
                }
            }).start();
        });

    }

    public void Q2Hbase(Route routerQ2){

        routerQ2.handler(routingContext -> {
            String userid = routingContext.request().getParam("userid");
            String tweet_time = routingContext.request().getParam("tweet_time");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Q2.lookupHbase(userid, tweet_time, routingContext);
                }
            }).start();
        });
    }



    public void Q3Mysql(SQLConnection connection, Route routerQ3) {

        routerQ3.handler(routingContext -> {
            String start_date = routingContext.request().getParam("start_date");
            String end_date = routingContext.request().getParam("end_date");
            String userid = routingContext.request().getParam("userid");
            String n = routingContext.request().getParam("n");

            new Thread(new Runnable() {
                @Override
                public void run() {
                    Q3.lookupMysql(connection, start_date, end_date, userid, n, routingContext);
                }
            }).start();
        });
    }

    public void Q3Hbase(Route routerQ3){

        routerQ3.handler(routingContext -> {
            String start_date = routingContext.request().getParam("start_date");
            String end_date = routingContext.request().getParam("end_date");
            String userid = routingContext.request().getParam("userid");
            String n = routingContext.request().getParam("n");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Q3.lookupHbase(start_date, end_date, userid, n,  routingContext);
                }
            }).start();
        });
    }



    public void Q4Mysql(SQLConnection connection, Route routerQ4) {

        routerQ4.handler(routingContext -> {
            String hashtag = routingContext.request().getParam("hashtag");
            String n = routingContext.request().getParam("n");

            new Thread(new Runnable() {
                @Override
                public void run() {
                    Q4.lookupMysql(connection, hashtag, n, routingContext);
                }
            }).start();
        });
    }

    public void Q4Hbase(Route routerQ4){

        routerQ4.handler(routingContext -> {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String hashtag = routingContext.request().getParam("hashtag");
                    String n = routingContext.request().getParam("n");
                    if (hashtag == null  || n == null){
                        routingContext.response()
                                .putHeader("Connection", "keep-alive")
                                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                                .end(ConfigSingleton.TEAMID + "," +
                                        ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n");
                    }
                    else
                        Q4.lookupHbase(hashtag, n, routingContext);
                }
            }).start();
        });
    }

    public void Q5(Route routerQ5) {

        routerQ5.handler(routingContext -> {
            long userid_min = Long.parseLong(routingContext.request().getParam("userid_min"));
            long userid_max = Long.parseLong(routingContext.request().getParam("userid_max"));

            Q5.getCount(userid_min, userid_max, routingContext);
        });
    }


    public static void main(String[] args) {
        config = ConfigSingleton.getInstance();
        config.superCache = new HashMap<String, String>();
        config.q4SuperCache = new HashMap<String, JSONArray>();
        Runner.runExample(CCTeam.class);
    }

    @Override
    public void start() throws Exception {
        // Initialize the mysql configuration
        if ( config.DATABASE.equals("mysql") ) {
            JsonObject mysqlConfig = new JsonObject()
                    .put("url", "jdbc:mysql://" + config.mysqlDNS + ":3306/" + config.mysqlDBName)
                    .put("driver_class", "com.mysql.jdbc.Driver")
                    .put("user", config.mysqlUser)
                    .put("password", config.mysqlPass)
                    .put("max_pool_size", config.MAX_MYSQL_CONNECTION);

            // initial the mysql Client
            config.mysqlClient = JDBCClient.createShared(vertx, mysqlConfig);
            System.out.println("\n\nMySql connected successfully!\n\n");

//            try {
//                // Register JDBC driver
//                Class.forName("com.mysql.jdbc.Driver");
//
//                // Open a connection
//                System.out.println("Connecting to database...");
//                config.mysqlQ4conn = DriverManager.getConnection("jdbc:mysql://" + config.mysqlDNS +
//                        ":3306/" + config.mysqlDBName, config.mysqlUser, config.mysqlPass);
//                config.mysqlQ4stmt = config.mysqlQ4conn.createStatement();
//
//            }catch(SQLException se){
//                //Handle errors for JDBC
//                se.printStackTrace();
//            }catch(Exception e){
//                //Handle errors for Class.forName
//                e.printStackTrace();
//            }

        }

        // Initialize the habse configuration
        else {
            Configuration conf;
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", config.hbaseMasterIP);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            //conf.set("hbase.rpc.timeout", "1800000");
            conf.set("hbase.master", config.hbaseMasterIP + ":60000");

            System.out.println("\n\nTrying to connect to HBase!\n\n");

            try {
                HBaseAdmin.checkHBaseAvailable(conf);
            } catch (MasterNotRunningException e) {
                System.out.println("\n\nHbase is not connected\n\n");
                System.exit(1);
            } catch (ZooKeeperConnectionException e) {
                System.out.println("\n\nZookeeper Problem\n\n");
                System.exit(1);
            }

            System.out.println("\n\nSuccessfully connected to HBase!\n\n");

            config.hbaseQ2Table = new HTable(conf, config.hbaseQ2TableName);
            config.hbaseQ3Table = new HTable(conf, config.hbaseQ3TableName);
            config.hbaseQ4Table = new HTable(conf, config.hbaseQ4TableName);

            System.out.println("\n\nHtable Connected!!\n\n");

        }

        // Start Testing our Q2 tester
//        Q2tester.start();

        Route routerHome = router.route("/");
        home(routerHome);

        Route routerQ1 = router.route("/q1");
        Q1(routerQ1);

        Route routerQ2 = router.route("/q2");
        if ( config.DATABASE.equals("mysql") ) {
            // open a single connection for all of our q2 requests
            config.mysqlClient.getConnection(res -> {
                if (res.succeeded()) {
                    final SQLConnection connection = res.result();
                    Q2Mysql(connection, routerQ2);
                } else {
                    // Failed to get connection - deal with it
                }
            });
        }
        else {
            Q2Hbase(routerQ2);
        }

        Route routerQ3 = router.route("/q3");
        if ( config.DATABASE.equals("mysql") ) {
            // open a single connection for all of our q3 requests
            config.mysqlClient.getConnection(res -> {
                if (res.succeeded()) {
                    final SQLConnection connection = res.result();
                    Q3Mysql(connection, routerQ3);
                } else {
                    // Failed to get connection - deal with it
                }
            });
        }
        else {
            Q3Hbase(routerQ3);
        }

        Route routerQ4 = router.route("/q4");
        if ( config.DATABASE.equals("mysql") ) {
            // open a single connection for all of our q4 requests
            config.mysqlClient.getConnection(res -> {
                if (res.succeeded()) {
                    final SQLConnection connection = res.result();
                    Q4Mysql(connection, routerQ4);
                } else {
                    // Failed to get connection - deal with it
                }
            });
        }
        else {
            Q4Hbase(routerQ4);
        }

        Route routerQ5 = router.route("/q5");
        Q5(routerQ5);

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);

    }

}

