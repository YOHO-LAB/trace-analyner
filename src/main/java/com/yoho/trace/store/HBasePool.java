package com.yoho.trace.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.PropertiesFactory;

import java.io.IOException;

/**
 * Created by xjipeng on 2017/10/16.
 */
public class HBasePool {

    private static final Logger logger = LoggerFactory.getLogger(TraceSpanStore.class) ;

    /**
     * hbase connection
     */
    private static Connection connection = null ;


    public static synchronized Connection getConnection(){
        if( connection == null ){
            try {
                //PropertiesFactory.initProperties();

                // init hbase config
                Configuration hbaseConf = HBaseConfiguration.create();
                hbaseConf.set("hbase.zookeeper.quorum", PropertiesFactory.hbase().getZkquorum());
                hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConf.set("hbase.defaults.for.version.skip", "true");

                connection = ConnectionFactory.createConnection(hbaseConf);

                logger.info("create hbase connection success {}, zk {} ", connection, PropertiesFactory.hbase().getZkquorum() );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection ;
    }


    /**
     *
     * @param tableName
     */
    public static void truncateTable(String tableName){
        Table table = null ;
        try {


            logger.info("start truncateTable {}", tableName);

            table = HBasePool.getConnection().getTable(TableName.valueOf(tableName));
            HTableDescriptor td = table.getTableDescriptor();

            HBasePool.getConnection().getAdmin().disableTable(TableName.valueOf(tableName));
            HBasePool.getConnection().getAdmin().truncateTable(TableName.valueOf(tableName),false);
            HBasePool.getConnection().getAdmin().enableTable(TableName.valueOf(tableName));

            logger.info("after truncateTable {} ", tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close() ;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
