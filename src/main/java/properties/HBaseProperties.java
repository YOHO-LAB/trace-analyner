package properties;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by xjipeng on 2017/10/12.
 */
public class HBaseProperties implements Serializable {
    private String zkquorum ;

    public String getZkquorum(){
        return this.zkquorum ;
    }

    public void setZkquorum(String zkquorum){
        this.zkquorum = zkquorum ;
    }
}
