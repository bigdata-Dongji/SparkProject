import java.text.SimpleDateFormat;
import java.util.Date;

public class demo {
    public static void main(String[] args) {
//        String dateTime = "2020-01-13T16:00:00.000Z";//"2013-12-01T00:00"

//        dateTime = dateTime.replace("Z", " UTC");
        String dateTime = "2013-12-01T00:00";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        SimpleDateFormat defaultFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date time = format.parse(dateTime);
            String result = defaultFormat.format(time);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
