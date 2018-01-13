package Utill;
import java.io.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class Utilities {
    private final static String LOG_FILE_NAME = "IdcDm.log";
    private static FileWriter logFileWriter;
    private static BufferedWriter logFileOutPutStream;

    // Log format for tracking and debugging
    public static void Log(String i_moduleName ,String i_Message) {
        if(logFileOutPutStream == null){
            try {
                logFileWriter = new FileWriter(LOG_FILE_NAME);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logFileOutPutStream = new BufferedWriter(logFileWriter);
        }
        Date timeStamp = new Date(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String logMessage = timeStamp + " " + sdf.format(cal.getTime())+" - " + i_moduleName + " " + i_Message + "\n";
        try {
            logFileOutPutStream.write(logMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Error Log format for tracking and debugging
    public static void ErrorLog(String i_moduleName ,String i_Message){
        if(logFileOutPutStream == null){
            try {
                logFileWriter = new FileWriter(LOG_FILE_NAME);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logFileOutPutStream = new BufferedWriter(logFileWriter);
        }
        Date timeStamp = new Date(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String logMessage = timeStamp + ": ERROR: " + sdf.format(cal.getTime())+" - " + i_moduleName + " " + i_Message + "\n";
        try {
            logFileWriter.write(logMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeLogFile(){
        try {
            logFileWriter.close();
            logFileOutPutStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}