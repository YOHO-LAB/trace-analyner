package properties;

import lombok.Data;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Created by markeloff on 2017/7/26.
 */
public class PropertiesFactory implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFactory.class);
    private static final String APP_YML_FILE = "app.yml";

    private static AllProperties properties;

    static {
        initProperties();
    }

    public static void initProperties() {
        loadAppYml();
    }

    public static AllProperties all() {
        return properties;
    }

    public static ESProperties es() {
        return properties.getEs();
    }

    public static KafkaProperties kafka() {
        return properties.getKafka();
    }

    public static SparkProperties spark() {
        return properties.getSpark();
    }

    public static HBaseProperties hbase() { return properties.getHbase(); }

    private static void loadAppYml() {
        logger.info("enter loadAppYml");
        InputStream inputStream = null;
        try {
            inputStream = PropertiesFactory.class.getClassLoader().getResourceAsStream(APP_YML_FILE);
            if (inputStream == null) {
                logger.error("inputStream is null");
            }
            Yaml yaml = new Yaml();
            properties = yaml.loadAs(inputStream, AllProperties.class);
            logger.info("properties: {} ", properties  );

        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        logger.info("loadAppYml finish");
    }
}
