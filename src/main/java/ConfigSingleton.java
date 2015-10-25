import io.vertx.ext.jdbc.JDBCClient;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Shimin Wang (andrewid: shiminw) on 15/10/16.
 *
 * This is a Singleton recordING configuration for our server
 */
public class ConfigSingleton {
    static final String TEAMID = "RXS";
    static final String TEAM_AWS_ACCOUNT_ID = "605262690154";

    static final String TEST_SERVER = "http://ec2-54-147-18-88.compute-1.amazonaws.com";

    static final String Q1DBName = "twitter";
    static final String Q1TableName = "tweetscore";

    static final String mysqlUser = "root";
    static final String mysqlPass = "1314159";
    static JDBCClient mysqlClient = null;

    static final int MAX_MYSQL_CONNECTION = 150;
//    static final int Q2_MAX_CONNECTION = MAX_MYSQL_CONNECTION;
    static final int Q2_MAX_CONNECTION = 10;
    static final int BLOCKING_QUEUE_SIZE = 10000/Q2_MAX_CONNECTION;

    private static volatile ConfigSingleton instance = null;
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
