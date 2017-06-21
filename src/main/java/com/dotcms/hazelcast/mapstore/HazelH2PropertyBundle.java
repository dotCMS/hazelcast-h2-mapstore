package com.dotcms.hazelcast.mapstore;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class reads configuration values from config.properties file.
 */
public class HazelH2PropertyBundle {
    private static final String PROPERTY_FILE_NAME = "H22MapStore.properties";
    private static Properties properties;
    static {
        properties = new Properties();
        try {
            InputStream in = HazelH2PropertyBundle.class
                            .getResourceAsStream("/" + PROPERTY_FILE_NAME);
            if (in == null) {
                in = HazelH2PropertyBundle.class.getResourceAsStream(
                                "/com/dotcms/hazelcast/mapstore/" + PROPERTY_FILE_NAME);
                if (in == null) {
                    throw new FileNotFoundException(PROPERTY_FILE_NAME + " not found");
                }
            }
            properties.load(in);
        } catch (FileNotFoundException e) {
            System.out.println("HazelH2PropertyBundle FileNotFoundException : " + PROPERTY_FILE_NAME + " not found");

        } catch (IOException e) {
            System.out.println("HazelH2PropertyBundle IOException : Can't read " + PROPERTY_FILE_NAME);

        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        String x = properties.getProperty(key);
        return (x == null) ? defaultValue : x;
    }

    public static int getIntProperty (String name, int defaultVal) {

        try{
            return Integer.parseInt(getProperty(name));
        }
        catch(Exception e){
            return defaultVal;
        }
    }

    public static boolean getBooleanProperty(String name, boolean defaultVal) {
        try{
            return Boolean.parseBoolean(getProperty(name));
        }
        catch(Exception e){
            return defaultVal;
        }
    }
}


