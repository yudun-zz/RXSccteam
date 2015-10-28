import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.util.HashMap;

/**
 * Created by yudun on 15/10/17.
 */
public class CCTeam extends AbstractVerticle {

    static ConfigSingleton config = null;

    Router router = Router.router(vertx);


    public void home(Route routerHome){
        routerHome.handler(routingContext -> {
            routingContext.response().end("<h1>Hello, We are " + config.TEAMID + "!" +
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

    public void Q2Mysql(Route routerQ2){
        routerQ2.handler(routingContext -> {
            String userid = routingContext.request().getParam("userid");
            String tweet_time = routingContext.request().getParam("tweet_time");

            Q2.lookupMysql(userid, tweet_time, routingContext);
        });
    }

    public void Q2Hbase(Route routerQ2){
        routerQ2.handler(routingContext -> {
            String userid = routingContext.request().getParam("userid");
            String tweet_time = routingContext.request().getParam("tweet_time");

            Q2.lookupHbase(userid, tweet_time, routingContext);
        });
    }


    public static void main(String[] args) {
        config = ConfigSingleton.getInstance();
        config.superCache = new HashMap<String, String>();
        Runner.runExample(CCTeam.class);
    }

    @Override
    public void start() throws Exception {
        // Initialize the mysql configuration
        if ( config.DATABASE.equals("mysql") ) {
            JsonObject mysqlConfig = new JsonObject()
                    .put("url", "jdbc:mysql://" + config.MysqlDNS + ":3306/" + config.Q2DBName)
                    .put("driver_class", "com.mysql.jdbc.Driver")
                    .put("user", config.mysqlUser)
                    .put("password", config.mysqlPass)
                    .put("max_pool_size", config.MAX_MYSQL_CONNECTION)
                    .put("min_pool_size", config.MAX_MYSQL_CONNECTION)
                    .put("initial_pool_size", config.MAX_MYSQL_CONNECTION);

            // initial the mysql Client
            config.mysqlClient = JDBCClient.createShared(vertx, mysqlConfig);
        }
        // Initialize the habse configuration
        else {
            Configuration conf;
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", config.HbaseMasterIP);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            //conf.set("hbase.rpc.timeout", "1800000");
            conf.set("hbase.master", config.HbaseMasterIP + ":60000");

            System.out.println("Start testing");

            try {
                HBaseAdmin.checkHBaseAvailable(conf);
            } catch (MasterNotRunningException e) {
                System.out.println("Hbase is not connected");
                System.exit(1);
            } catch (ZooKeeperConnectionException e) {
                System.out.println("Zookeeper Problem");
                System.exit(1);
            }

            System.out.println("Successfully connected to Hbase");

            config.tweetTable = new HTable(conf, config.HbaseQ2TableName);

        }

        // Start Testing our Q2 tester
//        Q2tester.start();

        Route routerHome = router.route("/");
        home(routerHome);

        Route routerQ1 = router.route("/q1");
        Q1(routerQ1);

        Route routerQ2 = router.route("/q2");

        if ( config.DATABASE.equals("mysql") ) {
            Q2Mysql(routerQ2);
        }
        else {
            Q2Hbase(routerQ2);
        }

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);

    }

}

