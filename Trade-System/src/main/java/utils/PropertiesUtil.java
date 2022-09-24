package utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class PropertiesUtil {

    static ParameterTool parameterTool = null;

    static {
        try {
            InputStream in = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("storage.properties")).openStream();
            parameterTool = ParameterTool.fromPropertiesFile(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ParameterTool getProperties() {
        return parameterTool;
    }

}
