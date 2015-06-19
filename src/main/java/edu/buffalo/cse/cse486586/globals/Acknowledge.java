package edu.buffalo.cse.cse486586.globals;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by archana on 4/21/15.
 */
public class Acknowledge implements Serializable{
    private HashMap<String,String> data;
    public Acknowledge(){
        this.data=null;
    }

    public void setData(HashMap<String, String> data){
        this.data= data;
    }
    public HashMap<String,String> getData(){
        return this.data;
    }
}
