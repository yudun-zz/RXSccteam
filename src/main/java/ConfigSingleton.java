import io.vertx.ext.jdbc.JDBCClient;
import org.apache.hadoop.hbase.client.HTable;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Shimin Wang (andrewid: shiminw) on 15/10/16.
 *
 * This is a Singleton recordING configuration for our server
 */
public class ConfigSingleton {
    private static volatile ConfigSingleton instance = null;

    // Team information
    static final String TEAMID = "RXS";
    static final String TEAM_AWS_ACCOUNT_ID = "605262690154";

    static final String TEST_SERVER = "http://ec2-54-147-18-88.compute-1.amazonaws.com";



    // Which database we gonna use? "mysql" or "hbase"
    static final String DATABASE = "mysql";



    //Config for mysql
    static final String MysqlDNS = "localhost";
//    static final String MysqlDNS = "54.174.202.217";
    static final String Q2DBName = "twitter";
    static final String Q2TableName = "tweetscore";

    static final String mysqlUser = "root";
    static final String mysqlPass = "1314159";
    static JDBCClient mysqlClient = null;

    static final int MAX_MYSQL_CONNECTION = 900;
    //    static final int Q2_MAX_CONNECTION = MAX_MYSQL_CONNECTION;
    static final int Q2_MAX_CONNECTION = 10;
    static final int BLOCKING_QUEUE_SIZE = 10000/Q2_MAX_CONNECTION;


    //Config for Hbase
    static final String HbaseMasterIP = "";
//    static final String HbaseMasterIP = "127.0.0.1";

    static final String HbaseQ2TableName = "mytweet";
    static final String HbaseQ2FamilyName = "TF";
    static final String HbaseQ2ColumnName = "content";

    static HTable tweetTable = null;


    //Super Cache -- cache every request we got
    static HashMap<String, String> superCache = null;

    AtomicInteger connectionCounter = null;

    ConfigSingleton() {
        connectionCounter = new AtomicInteger();
    }


    public static ConfigSingleton getInstance() {
        if (instance == null) {
            synchronized (ConfigSingleton.class) {
                // Double check to avid thread race
                if (instance == null) {
                    instance = new ConfigSingleton();
                }
            }
        }
        return instance;
    }
}
