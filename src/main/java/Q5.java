import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;

/**
 * Created by Shimin Wang on 11/26/15.
 */
public class Q5 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    private static int getMaxLess(ArrayList<ConfigSingleton.TweetCound> data, long target, int len) {
        int low = 0;
        int high = len - 1;

        while (high >= low) {

            int mid = (low + high) / 2;

            if (target < data.get(mid).uid)         //update index of the
                high = mid - 1;           //right-most element considered

            else if (target > data.get(mid).uid)    //update index of
                low = mid + 1;            //left-most element considered

            else {
//                System.out.println( mid-1 + ":"+ data.get(mid - 1).accumulateCount + ", " +
//                        mid + ":"+ data.get(mid).accumulateCount);

                return data.get(mid - 1).accumulateCount;
            }
        }

//        System.out.println(data.get(low-1).accumulateCount + " " + data.get(low).accumulateCount +  " " + data.get(low).accumulateCount);

        if (data.get(low).uid < target)
            return data.get(low).accumulateCount;
        else
            return data.get(low-1).accumulateCount;
    }

    private static int getMaxLessEqual(ArrayList<ConfigSingleton.TweetCound> data, long target, int len) {
        int low = 0;
        int high = len - 1;

        while (high >= low) {

            int mid = (low + high) / 2;

            if (target < data.get(mid).uid)         //update index of the
                high = mid - 1;           //right-most element considered

            else if (target > data.get(mid).uid)    //update index of
                low = mid + 1;            //left-most element considered

            else
                return data.get(mid).accumulateCount;
        }

        if (data.get(low).uid > target)
            return data.get(low-1).accumulateCount;
        else
            return data.get(low).accumulateCount;
    }

    static ConfigSingleton config = ConfigSingleton.getInstance();
    public static void getCount(long userid_min, long userid_max,
                                   RoutingContext routingContext){

        StringBuilder result = new StringBuilder(resultHeader);
        int count = 0;
        int lowerBound, upperBound;
        if (userid_min <= userid_max) {
            if (userid_min <= config.minUid)
                lowerBound = 0;
            else
                lowerBound = getMaxLess(config.q5SuperCache, userid_min, config.q5DataNum);

            if (userid_max >= config.maxUid)
                upperBound = config.maxCount;
            else
                upperBound = getMaxLessEqual(config.q5SuperCache, userid_max, config.q5DataNum);

            count = upperBound - lowerBound;
        }

        result.append(count + "\n");
        routingContext.response()
                .putHeader("Connection", "keep-alive")
                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                .end(result.toString());
    }
}
