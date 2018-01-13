package Utill;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class Utilities {

    // Log format for tracking and debugging
    public static void Log(String i_moduleName ,String i_Message)
    {
        Date timeStamp = new Date(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String logMessage = timeStamp + " " + sdf.format(cal.getTime())+" - " + i_moduleName + " " + i_Message;
       // System.out.println(logMessage);
    }

    // Error Log format for tracking and debugging
    public static void ErrorLog(String i_moduleName ,String i_Message)
    {
        Date timeStamp = new Date(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String logMessage = timeStamp + " " + sdf.format(cal.getTime())+" - " + i_moduleName + " " + i_Message;
        //System.err.println(logMessage);
    }

}