package edu.buffalo.cse.cse486586.tasks;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import edu.buffalo.cse.cse486586.globals.Acknowledge;
import edu.buffalo.cse.cse486586.globals.Constants;
import edu.buffalo.cse.cse486586.globals.MessagePacket;
import edu.buffalo.cse.cse486586.globals.Variables;
import edu.buffalo.cse.cse486586.utilities.DynamoUtilities;
import static edu.buffalo.cse.cse486586.globals.Constants.DELETE;
import static edu.buffalo.cse.cse486586.globals.Constants.G_QUERY;
import static edu.buffalo.cse.cse486586.globals.Constants.INSERT_Q;
import static edu.buffalo.cse.cse486586.globals.Constants.QUERYQ;
import static edu.buffalo.cse.cse486586.globals.Constants.QUERY_REPLICATE_Q;
import static edu.buffalo.cse.cse486586.globals.Constants.RECOVERY;
import static edu.buffalo.cse.cse486586.globals.Constants.REPLICA_Q;
import static edu.buffalo.cse.cse486586.globals.Variables.*;

/**
 * Created by archana on 4/13/15.
 */
public class ServerTask extends AsyncTask<ServerSocket, MessagePacket, Void> {
    @Override
    protected Void doInBackground(ServerSocket... params) {
        ServerSocket serverSocket = params[0];
        try{
            while(true){
                Log.v("SERVER", "Listening");
                Socket socket = serverSocket.accept();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                MessagePacket Incoming = (MessagePacket) in.readObject();
                if(Incoming!=null){
                    Log.e("SERVER", "Received a message from of type"+Incoming.getType());
                    Incoming.setSocket(socket);
                    //processMessage(Incoming,socket);
                    new ProcessMessage().doInBackground(Incoming);
                }
            }

        }catch (ClassNotFoundException e) {
            Log.e("SERVER", "Class Not Found");
            e.printStackTrace();
        } catch (OptionalDataException e) {
            Log.e("SERVER", "Data not found");
            e.printStackTrace();
        } catch (StreamCorruptedException e) {
            Log.e("SERVER", "Stream Not found");
            e.printStackTrace();
        } catch (IOException e) {
            Log.e("SERVER", "IO");
            e.printStackTrace();
        }
        return null;
    }

    private class ProcessMessage  {

        protected synchronized Void doInBackground(MessagePacket... params) {
            MessagePacket incoming =params[0];
            String type = incoming.getType();
            Socket clientSocket = incoming.getSocket();
            Log.e("PROCESS",type);

            if(type.equals(INSERT_Q)||type.equals(REPLICA_Q)){
                //insert at right owner
                Log.e("QIS","insert of "+type+" myport"+myPort);
                Variables.uri=callInsertSelf(incoming);
                /*Acknowledge ack =new Acknowledge();
                Log.e("QIS","send ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }*/

            }/*else if(type.equals(REPLICA_Q)){
                //replicate
                Log.e("QIRS","insert replica"+incoming.getOwner()+" myport"+myPort);
                Variables.uri=callInsertSelf(incoming);
                Acknowledge ack =new Acknowledge();
                Log.e("QIRS","send replica ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }*/else if(type.equals(QUERYQ)){
                //query owner
                Log.e("QQSS","query owner"+incoming.getOwner()+" myport"+myPort);
                while(loading);
                Cursor cursor= callQuerySelf(incoming);
                Acknowledge ack = new Acknowledge();
                ack.setData(DynamoUtilities.getDatafromCursor(cursor));
                Log.e("QQSS","send ack");
                try {
                   ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if(type.equals(G_QUERY)||type.equals(RECOVERY)||type.equals(QUERY_REPLICATE_Q)){
                //replicate
                Log.e("testserver",type);
                Log.e("QQGS","query for global with key "+incoming.getKey()+"at "+incoming.getOwner()+" myport"+myPort);
                Cursor cursor= callQuerySelf(incoming);
                Acknowledge ack = new Acknowledge();
                ack.setData(DynamoUtilities.getDatafromCursor(cursor));
                Log.e("QQGS","send ack to originator "+incoming.getOwner());
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if(type.equals(DELETE)){
                Log.e("QDS","delete"+incoming.getKey()+" myport"+myPort);
                DynamoUtilities.delete_self(incoming.getKey());
            }

            return null;
        }
    }
    private Cursor callQuery(MessagePacket messagePacket){
        Log.e("QQ_C","CALL QUERY"+ myPort);
        return context.getContentResolver().query(Variables.uri,null,messagePacket.getKey(),null,null);
    }

    private Cursor callQuerySelf(MessagePacket messagePacket){
        Log.e("QQ_S","CALL SELF QUERY"+ myPort);
        return DynamoUtilities.query_self(messagePacket.getKey());
    }

    private Uri callInsert(MessagePacket incoming) {
        Log.e("II_C","CALL INSERT OF"+ myPort);
        ContentValues keyValueToInsert = new ContentValues();
        keyValueToInsert.put(Constants.KEY_FIELD, incoming.getKey());
        keyValueToInsert.put(Constants.VALUE_FIELD,incoming.getValue());
        return context.getContentResolver().insert(Variables.uri,keyValueToInsert);
    }

    private Uri callInsertSelf(MessagePacket incoming) {
        Log.e("II_S","CALL INSERT OF for insert type"+incoming.getType()+"at "+ myPort);
        ContentValues keyValueToInsert = new ContentValues();
        keyValueToInsert.put(Constants.KEY_FIELD, incoming.getKey());
        keyValueToInsert.put(Constants.VALUE_FIELD,incoming.getValue());
        return DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
    }
}
