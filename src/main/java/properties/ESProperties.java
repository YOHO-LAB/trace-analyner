package properties;

import java.io.Serializable;
import java.util.List;

/**
 * Created by markeloff on 2017/7/26.
 */
public class ESProperties implements Serializable{
    private String cluster;
    private List<String> hosts;
    private String index;
    private int indexShards;
    private int indexReplicas;


    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public int getIndexShards() {
        return indexShards;
    }

    public void setIndexShards(int indexShards) {
        this.indexShards = indexShards;
    }

    public int getIndexReplicas() {
        return indexReplicas;
    }

    public void setIndexReplicas(int indexReplicas) {
        this.indexReplicas = indexReplicas;
    }

}
