package properties;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by markeloff on 2017/7/26.
 */
public class AllProperties implements Serializable {
    private SparkProperties spark;
    private ESProperties es;
    private KafkaProperties kafka;
    private HBaseProperties hbase;

    public ESProperties getEs() {
        return es;
    }

    public void setEs(ESProperties es) {
        this.es = es;
    }

    public KafkaProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public SparkProperties getSpark() {
        return spark;
    }

    public HBaseProperties getHbase() {
        return hbase;
    }

    public void setHbase(HBaseProperties hbase){
        this.hbase = hbase ;
    }

    public void setSpark(SparkProperties spark) {
        this.spark = spark;
    }


}
