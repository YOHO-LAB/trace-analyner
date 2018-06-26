package properties;

import java.io.Serializable;

/**
 * Created by markeloff on 2017/7/26.
 */
public class SparkProperties implements Serializable {
    private int duration;

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }
}
