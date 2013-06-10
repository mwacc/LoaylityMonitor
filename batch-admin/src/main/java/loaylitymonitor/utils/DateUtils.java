package loaylitymonitor.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    private DateUtils(){}

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    public static String getFormdatedDate(Date date) {
        return df.format(date);
    }

}