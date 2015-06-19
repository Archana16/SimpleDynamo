package edu.buffalo.cse.cse486586.globals;

import android.net.Uri;

/**
 * Created by archana on 4/13/15.
 */
public class Constants {

    public static final String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
    public static final String[] PORTS = {"5554","5556","5558","5560","5562"};
    public static final int SERVER_PORT = 10000;
    public static final String DB_NAME ="dynamo_DB";
    public static final String TABLE_NAME ="dynamo_Table";
    public static final String DB_PATH = "data/data/edu.buffalo.cse.cse486586/simpledynamo/databases/"+DB_NAME;
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String VERSION_FIELD = "value";


    public static final String REPLICA_Q ="replicate_q";
    public static final String INSERT_Q= "insert_q";

    public static final String LOCAL_INDICATOR = "\"@\"";
    public static final String GLOBAL_INDICATOR = "\"*\"";
    public static final String G_QUERY ="globalQuery";
    public static final String QUERYQ ="queryq";
    public static final String QUERY_REPLICATE_Q ="query_replicate";
    public static final String RECOVERY ="recovery";
    public static final String DELETE = "delete";

   /* public static final String INSERT = "insert";
    public static final String REPLICA1 ="replicate1";
    public static final String REPLICA2 ="replicate2";
    public static final String QUERY ="query";
    public static final String QUERY_REPLICATE ="query_replicate";
    public static final String ACK="acknowledge";*/

}
