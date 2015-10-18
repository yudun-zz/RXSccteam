/**
 * Created by Shimin Wang (andrewid: shiminw) on 15/10/16.
 *
 * This is a Singleton recordING configuration for our server
 */
public class ConfigSingleton {
    static final String TEAMID = "RXS";
    static final String TEAM_AWS_ACCOUNT_ID = "605262690154";


    private static volatile ConfigSingleton instance = null;


    ConfigSingleton() {

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
