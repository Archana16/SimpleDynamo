package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import edu.buffalo.cse.cse486586.globals.Acknowledge;
import edu.buffalo.cse.cse486586.globals.MessagePacket;
import edu.buffalo.cse.cse486586.globals.Variables;
import edu.buffalo.cse.cse486586.tasks.ServerTask;
import edu.buffalo.cse.cse486586.utilities.DynamoUtilities;

import static edu.buffalo.cse.cse486586.globals.Constants.*;
import static edu.buffalo.cse.cse486586.globals.Variables.*;
import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.checkNode;
import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.delete_self;
import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.genHash;
import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.insert_self;
import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.query_self;

public class SimpleDynamoProvider extends ContentProvider {
    //public static Cursor cObj;

    @Override
    public boolean onCreate() {
        Log.e("onCreate", "App started");
        //INIT
        initialize();

        /*boolean recovery = false;
        recovery=DynamoUtilities.checkDB();*/


        //CREATE DB
        DynamoDB dbContext = new DynamoDB(context,DB_NAME,null,3);
        dynamoDB = dbContext.getWritableDatabase();
        if (dynamoDB==null)
            return false;
        else
            Log.e("test","newly db created");

        Log.e("test",idToPortMap.get(succ1)+idToPortMap.get(prev1));

       delete_self(LOCAL_INDICATOR);

        //CREATE SERVER TASK
        ServerTask serverTask;
        serverTask = new ServerTask();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        serverTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        //should be if recovery
        if(true){
            Log.e("test","recovering");
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setType(RECOVERY);
            messagePacket.setKey(LOCAL_INDICATOR);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
        }
        Log.e("onCreate", "Exiting onCreate");
        return true;
    }

    private void initialize() {

        Log.e("init","AVD Initialize");
        //IDENTIFY MY_PORT AND MY_ID
        context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr))*2);

        try {
            myID = genHash(String.valueOf(Integer.parseInt(portStr)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("init","my id and port in init "+myID+myPort);

        DynamoUtilities.createRingAndMap();
        DynamoUtilities.UpdateMaps();
        int N= ringList.size();
        for(int i =0; i <N;i++){
            if(ringList.get(i).equals(myID)){
                succ1 = ringList.get((i+1)%N);
                succ2 = ringList.get((i+2)%N);
                prev1 = ringList.get((i+(N-1))%N);
                prev2 = ringList.get((i+(N-2))%N);
                Log.e("init","succ1 "+succ1+"succ2 "+succ2+"prev1 "+prev1+"prev2 "+prev2);
                break;
            }else{
                continue;
            }
        }

        Log.e("init","Initialized");
    }


    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals(GLOBAL_INDICATOR)) {
            int rows = DynamoUtilities.delete_self(LOCAL_INDICATOR);
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setType(DELETE);
            messagePacket.setKey(LOCAL_INDICATOR);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
            return rows;
        }else if(selection.equals(LOCAL_INDICATOR)){
            int rows = DynamoUtilities.delete_self(LOCAL_INDICATOR);
            return rows;
        }else{
            Log.e("D TEST", "INSIDE string DELETE" + selection);
            int rows = DynamoUtilities.delete_self(selection);
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setType(DELETE);
            messagePacket.setKey(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
            return rows;
        }
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


    @Override
    public Uri insert(Uri uri, ContentValues values) {

        while(loading);


        Log.e("I","IN Insert of" +myPort);
        String key = values.get(KEY_FIELD).toString();
        String value = values.get(VALUE_FIELD).toString();
        String ownerKey = checkNode(key);
        String coordinator= idToPortMap.get(ownerKey);
        MessagePacket messagePacket = new MessagePacket();
        messagePacket.setOwner(coordinator);
        messagePacket.setKey(key);
        messagePacket.setValue(value);
        String succ1key= succesorMap.get(ownerKey);
        String succ2key = succesorMap.get(succ1key);
        messagePacket.setSucc1(idToPortMap.get(succ1key));
        messagePacket.setSucc2(idToPortMap.get(succ2key));
        if(coordinator.equals(myPort)) {
            //INSERT HERE
            Log.e("I_S", "I'm owner," + coordinator + "myport" + myPort);
            Uri newUri = insert_self(Variables.uri, values);
            messagePacket.setType(REPLICA_Q);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);

            /*
            messagePacket.setSendTo(messagePacket.getSucc1());
            Log.e("I_R", "fwd to succ1 " + messagePacket.getSucc1());
            try {
                forwardMessage(messagePacket);
                Log.e("I_R","replicated in the succ1");
            } catch (Exception e) {
                Log.e("I_R", "time out for first replica " + messagePacket.getSendTo());
            }

            //repetive
            messagePacket.setType(REPLICA_Q);
            messagePacket.setSendTo(messagePacket.getSucc2());
            Log.e("I_R", "fwd to succ2 " + messagePacket.getSucc2());
            try {
                forwardMessage(messagePacket);
                Log.e("I_R","replicated in the succ2");
            } catch (Exception e) {
                Log.e("I_R", "time out for second replica " + messagePacket.getSendTo());
            }*/
            return  newUri;
        }else{
            Log.e("I_R", "im "+myPort+"fwd to owner " + messagePacket.getOwner());
            messagePacket.setType(INSERT_Q);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);

            /*messagePacket.setSendTo(messagePacket.getOwner());

            try {
                forwardMessage(messagePacket);
                Log.e("I_R","inserted at the owner");
            } catch (Exception e) {
                Log.e("I_R", "time out at owner " + messagePacket.getOwner());
                Log.e("I_R", "directly replicating to successors ");

                messagePacket.setType(REPLICA_Q);
                messagePacket.setSendTo(messagePacket.getSucc1());
                Log.e("I_R", "fwd to succ1 of failed owner" + messagePacket.getSucc1());
                try {
                    forwardMessage(messagePacket);
                    Log.e("I_R","replicated in the succ1 of failed owner");
                } catch (Exception e1) {
                    Log.e("I_R", "!!!!!!!!!");
                }


                messagePacket.setType(REPLICA_Q);
                messagePacket.setSendTo(messagePacket.getSucc2());
                Log.e("I_R", "fwd to succ1 of failed owner" + messagePacket.getSucc2());
                try {
                    forwardMessage(messagePacket);
                    Log.e("I_R","replicated in the succ2 of failed owner");
                } catch (Exception e1) {
                    Log.e("I_R", "!!!!!!!!!");
                }
            }*/

            return null;
        }

    }






    private Acknowledge forwardMessage(MessagePacket messagePacket) throws Exception {
        // create socket and send message
        Log.e("FWD","SEND to" +messagePacket.getSendTo());
        Acknowledge acknowledge= null;
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messagePacket.getSendTo()));
                socket.setSoTimeout(700);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(messagePacket);
                Log.e("FWD","WAITING for ACK");
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                acknowledge = (Acknowledge) in.readObject();
            } catch (Exception e) {
                Log.e("FWD","timeout exception... was waiting for"+messagePacket.getSendTo());
                throw new Exception();
            }
            return acknowledge;

    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {



        Log.e("Q","INSIDE QUERY");
        cobj=query_self(LOCAL_INDICATOR);
        Log.v("Q", "queried and initialised cursor");

        while(loading);

        if(selection.equals(GLOBAL_INDICATOR)){
            Acknowledge ack = new Acknowledge();
            Log.e("Q_G", "INSIDE * QUERY");
            HashMap<String,String> global = DynamoUtilities.getDatafromCursor(cobj);
            MessagePacket messagePacket= new MessagePacket();
            for(String port:REMOTE_PORTS) {
                if (!port.equals(myPort)) {
                    messagePacket.setSendTo(port);
                    messagePacket.setType(G_QUERY);
                    messagePacket.setKey(LOCAL_INDICATOR);
                    try {
                        ack = forwardMessage(messagePacket);
                        Log.e("Q_G","data from "+messagePacket.getSendTo()+ack.getData().toString());
                        global.putAll(ack.getData());

                    } catch (Exception e) {
                        Log.e("Q_G", "query timed out at" + port);
                        e.printStackTrace();
                    }

                }
            }
            Log.e("Q_G",global.toString());
            MatrixCursor matrixCursor =DynamoUtilities.getCursorfromMap(global);
            return matrixCursor;
        }
        else if(selection.equals(LOCAL_INDICATOR)){
            Log.e("Q_L","INSIDE @ QUERY of"+myPort);
            return query_self(LOCAL_INDICATOR);
        }else {
            Log.e("Q_S", "INSIDE string QUERY" + selection);
            String ownerKey = checkNode(selection);
            String coordinator= idToPortMap.get(ownerKey);
            Log.e("Q_S","owner port "+coordinator+"my port "+myPort);
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setOwner(coordinator);
            messagePacket.setKey(selection);
            String succ1key= succesorMap.get(ownerKey);
            String succ2key = succesorMap.get(succ1key);
            messagePacket.setSucc1(idToPortMap.get(succ1key));
            messagePacket.setSucc2(idToPortMap.get(succ2key));
            Log.e("Q_S","message initialised owner "+messagePacket.getOwner()+"succ1 "+messagePacket.getSucc1()+"succ2 "+messagePacket.getSucc2());
            Acknowledge ack = new Acknowledge();
            if(!coordinator.equals(myPort)){
                // fwd to owner
                Log.e("Q_S", "NOT INSIDE MY DB");
                messagePacket.setSendTo(messagePacket.getOwner());
                messagePacket.setType(QUERYQ);
                messagePacket.setKey(selection);
                try {
                    ack=forwardMessage(messagePacket);
                    Log.e("Q_S","got from actual owner "+ack.getData().get(messagePacket.getKey()));
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                    matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                    return matrixCursor;
                } catch (Exception e) {
                    Log.e("Q_S", "timeout at coordinator");
                    //handle owner failure
                    messagePacket.setType(QUERY_REPLICATE_Q);
                    messagePacket.setSendTo(messagePacket.getSucc1());
                    try {
                        ack=forwardMessage(messagePacket);
                        Log.e("Q_S","got from owner's succ1 "+ack.getData().get(messagePacket.getKey()));
                    } catch (Exception e1) {
                        Log.e("Q_S", "timeout at succ1 !!!!!!!!!!!!!");
                        messagePacket.setType(QUERY_REPLICATE_Q);
                        messagePacket.setSendTo(messagePacket.getSucc2());
                        try {
                            ack=forwardMessage(messagePacket);
                            Log.e("Q_S","got from owner's succ2 "+ack.getData().get(messagePacket.getKey()));
                        } catch (Exception e2) {
                            Log.e("Q_S", "timeout at succ2 !!!!!!!!!!!!!");
                            return null;
                        }
                    }

                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                    matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                    return matrixCursor;
                }
            } else {
                Log.e("Q_S", "INSIDE MY DB");
                cobj = query_self(selection);
                return cobj;
            }
        }
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private class ClientTask extends AsyncTask<MessagePacket, Void, Void> {
        @Override
        protected Void doInBackground(MessagePacket... params) {
            MessagePacket messagePacket = params[0];
            Log.e("CLIENT", "NEW private client "+messagePacket.getType());
            if(messagePacket.getType().equals(RECOVERY)){
                //delete_self(LOCAL_INDICATOR);
                Acknowledge ack = new Acknowledge();
                Log.e("Rec", "REcovering");
                HashMap<String,String> global = new HashMap<String,String>();
                for(String port:REMOTE_PORTS) {
                    if (!port.equals(myPort)) {
                        messagePacket.setSendTo(port);
                        messagePacket.setKey(LOCAL_INDICATOR);
                        try {
                            ack = forwardMessage(messagePacket);
                            Log.e("Rec","data from ");
                            global.putAll(ack.getData());
                            for (Map.Entry<String, String> entry : global.entrySet()) {
                                String key = entry.getKey();
                                String owner = checkNode(key);
                                if(owner.equals(myID)||owner.equals(prev1)||owner.equals(prev2)){
                                    Log.e("CLIENT", key+"  "+entry.getValue());
                                    ContentValues keyValueToInsert = new ContentValues();
                                    keyValueToInsert.put(KEY_FIELD, key);
                                    keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                                    Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                                }


                            }

                        } catch (Exception e) {
                            Log.e("Rec", "query timed out at" + port);
                            e.printStackTrace();
                        }

                    }
                }
                loading=false;
            }
            /*if(messagePacket.getType().equals(RECOVERY)){
                messagePacket.setSendTo(idToPortMap.get(succ1));
                Log.e("CLIENT", "recover from succ1");
                try {
                    Acknowledge acknowledge = forwardMessage(messagePacket);
                    if(acknowledge!=null){
                        Log.e("CLIENT", "received");
                        HashMap<String,String> replica = acknowledge.getData();
                        for (Map.Entry<String, String> entry : replica.entrySet()) {
                            String key = entry.getKey();
                            String owner = checkNode(key);
                            if(owner.equals(myID)||owner.equals(prev1)||owner.equals(prev2)){
                                Log.e("CLIENT", key+"  "+entry.getValue());
                                ContentValues keyValueToInsert = new ContentValues();
                                keyValueToInsert.put(KEY_FIELD, key);
                                keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                                Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                            }


                        }

                    }
                } catch (Exception e) {
                    Log.e("recovery","pipe broken at succ1");
                    messagePacket.setSendTo(idToPortMap.get(succ2));
                    Log.e("CLIENT", "recover from succ2");
                    try {
                        Acknowledge acknowledge = forwardMessage(messagePacket);
                        if(acknowledge!=null){
                            Log.e("CLIENT", "received");
                            HashMap<String,String> replica = acknowledge.getData();
                            for (Map.Entry<String, String> entry : replica.entrySet()) {
                                String key = entry.getKey();
                                String owner = checkNode(key);
                                if(owner.equals(myID)||owner.equals(prev1)||owner.equals(prev2)){
                                    Log.e("CLIENT", key+"  "+entry.getValue());
                                    ContentValues keyValueToInsert = new ContentValues();
                                    keyValueToInsert.put(KEY_FIELD, key);
                                    keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                                    Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                                }


                            }

                        }
                    } catch (Exception e1) {
                        Log.e("recovery","pipe broken at succ2");
                    }
                }



                messagePacket.setSendTo(idToPortMap.get(prev1));
                Log.e("CLIENT", "recover from prev1");
                try {
                    Acknowledge acknowledge = forwardMessage(messagePacket);
                    if(acknowledge!=null){
                        Log.e("CLIENT", "received");
                        HashMap<String,String> replica = acknowledge.getData();
                        for (Map.Entry<String, String> entry : replica.entrySet()) {
                            String key = entry.getKey();
                            String owner = checkNode(key);
                            if(owner.equals(myID)||owner.equals(prev1)||owner.equals(prev2)){
                                Log.e("CLIENT", key+"  "+entry.getValue());
                                ContentValues keyValueToInsert = new ContentValues();
                                keyValueToInsert.put(KEY_FIELD, key);
                                keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                                Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                            }


                        }

                    }
                } catch (Exception e) {
                    Log.e("recovery","pipe broken at prev1");
                    messagePacket.setSendTo(idToPortMap.get(prev2));
                    Log.e("CLIENT", "recover from prev2");
                    try {
                        Acknowledge acknowledge = forwardMessage(messagePacket);
                        if(acknowledge!=null){
                            Log.e("CLIENT", "received");
                            HashMap<String,String> replica = acknowledge.getData();
                            for (Map.Entry<String, String> entry : replica.entrySet()) {
                                String key = entry.getKey();
                                String owner = checkNode(key);
                                if(owner.equals(myID)||owner.equals(prev1)||owner.equals(prev2)){
                                    Log.e("CLIENT", key+"  "+entry.getValue());
                                    ContentValues keyValueToInsert = new ContentValues();
                                    keyValueToInsert.put(KEY_FIELD, key);
                                    keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                                    Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                                }
                            }

                        }
                    } catch (Exception e1) {
                        Log.e("recovery","pipe broken at prev2");
                    }
                }
            }*/else if(messagePacket.getType().equals(INSERT_Q)){
                ArrayList<String> ports = new ArrayList<String>();
                ports.add(messagePacket.getOwner());
                ports.add(messagePacket.getSucc1());
                ports.add(messagePacket.getSucc2());
                for(String p : ports) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(p));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(messagePacket);
                        out.close();
                        socket.close();
                    }catch(Exception e){
                        Log.e("insert and replica",e.getMessage());
                    }

                }


            }else if(messagePacket.getType().equals(REPLICA_Q)){

                ArrayList<String> ports = new ArrayList<String>();

                ports.add(messagePacket.getSucc1());
                ports.add(messagePacket.getSucc2());
                for(String p : ports) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(p));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(messagePacket);
                        out.close();
                        socket.close();
                    }catch(Exception e){
                        Log.e("replica exception",e.getMessage());
                    }

                }
            }else{
                for(String p : REMOTE_PORTS) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(p));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(messagePacket);
                        out.close();
                        socket.close();
                    }catch(Exception e){
                        Log.e("delete exception",e.getMessage());
                    }

                }

            }

            return null;
        }
}
}
