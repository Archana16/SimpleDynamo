package edu.buffalo.cse.cse486586.globals;

import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by archana on 4/13/15.
 */
public class MessagePacket implements Serializable{
    private String type;
    private String key;
    private String value;
    private String owner;
    private String succ1;
    private String succ2;
    private String sendTo;
    private Socket socket;

    public MessagePacket(){
        this.type = null;
        this.key = null;
        this.value = null ;
        this.owner=null;
        this.succ1=null;
        this.succ2=null;
        this.sendTo=null;
        this.socket=null;

    }

    public String getType(){
        return this.type;
    }
    public void setType(String type){
        this.type = type;
    }
    public String getKey(){return this.key;}
    public void setKey(String key){ this.key = key;}
    public String getValue(){return this.value;}
    public void setValue(String value){this.value = value;}
    public String getOwner(){return this.owner;}
    public void setOwner(String owner){this.owner=owner;}
    public String getSucc1(){return this.succ1;}
    public void setSucc1(String succ1){this.succ1=succ1;}
    public String getSucc2(){return this.succ2;}
    public void setSucc2(String succ2){this.succ2=succ2;}
    public String getSendTo(){return this.sendTo;}
    public void setSendTo(String sendTo){this.sendTo= sendTo;}
    public Socket getSocket(){return this.socket;}
    public void setSocket(Socket socket){this.socket=socket;}
}
