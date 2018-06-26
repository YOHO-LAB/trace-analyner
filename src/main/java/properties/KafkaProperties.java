package properties;

import java.io.Serializable;

/**
 * Created by markeloff on 2017/7/26.
 */
public class KafkaProperties implements Serializable {
    private String topic;
    private String brokers;
    private String group;
    private int replicationFactor;

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
