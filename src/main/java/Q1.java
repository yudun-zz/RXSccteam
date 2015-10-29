import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yudun on 15/10/17.
 */
public class Q1 {

    final static BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
    final static BigInteger big25 = new BigInteger("25");

    final static String resultHeaderP1 = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    private static char decode(char c, int Z){
        if (c - 'A' + 1 > Z)
            return (char)(c - Z);
        else
            return (char)(c + 26 - Z);
    }

    public static String decrypt(String key, String message){

        BigInteger XY = new BigInteger(key);
        String C = message;

        int Z = (XY.divide(X).mod(big25)).intValue() + 1;

        int len = C.length();

        StringBuilder result = new StringBuilder(resultHeaderP1);
        result.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date()) + "\n");

        int aSize = (int)(Math.sqrt(len));
        char[][] array = new char[aSize][aSize];

        int index = 0;
        for (int i=0; i<aSize; i++){
            for (int j=0; j<aSize; j++){
                array[i][j] = C.charAt(index);
                index ++;
            }
        }

        int x=0, y=0;

        while (y < aSize-1){
            int currentY = y;
            int currentX = x;
            while(currentY >= 0 && currentX < aSize){
                result.append(decode(array[currentX][currentY], Z));
                currentX ++;
                currentY --;
            }
            y++;
        }

        while (x < aSize){
            int currentY = y;
            int currentX = x;
            while(currentY >= 0 && currentX < aSize){
                result.append(decode(array[currentX][currentY], Z));
                currentX ++;
                currentY --;
            }
            x++;
        }

        result.append("\n");
        return result.toString();
    }

}
