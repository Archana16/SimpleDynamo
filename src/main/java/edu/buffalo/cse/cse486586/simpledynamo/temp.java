package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import static edu.buffalo.cse.cse486586.globals.Variables.first;
import static edu.buffalo.cse.cse486586.globals.Variables.last;

/**
 * Created by archana on 5/1/15.
 */
public class temp {
    /*private class Recovery extends AsyncTask<MessagePacket, String, Void> {
        @Override
        protected Void doInBackground(MessagePacket... params) {
            MessagePacket messagePacket = params[0];
            messagePacket.setSendTo(idToPortMap.get(prev1));
            try {
                Acknowledge acknowledge = forwardMessage(messagePacket);
                if(acknowledge!=null){
                HashMap<String,String> replica = acknowledge.getData();
                    for (Map.Entry<String, String> entry : replica.entrySet()) {
                        String key = entry.getKey();
                        if(checkNode(key).equals(prev1)||checkNode(key).equals(prev2)){
                            ContentValues keyValueToInsert = new ContentValues();
                            keyValueToInsert.put(KEY_FIELD, key);
                            keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                            Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                        }

                    }

                }

            } catch (SocketTimeoutException e) {
                Log.e("onrecover","!!!!!!!!!");
                // e.printStackTrace();
            }
            messagePacket.setSendTo(idToPortMap.get(succ1));
            try {
                Acknowledge acknowledge = forwardMessage(messagePacket);
                if(acknowledge!=null){
                HashMap<String, String> replica = acknowledge.getData();
                    for (Map.Entry<String, String> entry : replica.entrySet()) {
                        String key = entry.getKey();
                        if (checkNode(key).equals(myID)) {
                            ContentValues keyValueToInsert = new ContentValues();
                            keyValueToInsert.put(KEY_FIELD, key);
                            keyValueToInsert.put(VALUE_FIELD, entry.getValue());
                            Variables.uri = DynamoUtilities.insert_self(Variables.uri, keyValueToInsert);
                        }
                    }
            }
            } catch (SocketTimeoutException e) {
                Log.e("onrecover","!!!!!!!!!");
                // e.printStackTrace();
            }
            return null;
        }
    }
    private void callRecovery() {

        Log.e("recovering","at port "+myPort);

        //insert all prev1 and prev2 key-values
        MessagePacket messagePacket = new MessagePacket();
        messagePacket.setType(QUERYQ);
        messagePacket.setKey(LOCAL_INDICATOR);
        messagePacket.setSendTo(idToPortMap.get(prev1));
        try {
            Acknowledge acknowledge = forwardMessage(messagePacket);
            HashMap<String,String> replica = acknowledge.getData();
            for (Map.Entry<String, String> entry : replica.entrySet()) {
                String key = entry.getKey();
                if(checkNode(key).equals(prev1)||checkNode(key).equals(prev2)){
                    ContentValues keyValueToInsert = new ContentValues();
                    keyValueToInsert.put(KEY_FIELD, key);
                    keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                    Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                }

            }
        } catch (SocketTimeoutException e) {
            Log.e("onrecover", "!!!!!!!!!");
           // e.printStackTrace();
        }
        messagePacket.setSendTo(idToPortMap.get(succ1));
        try {
            Acknowledge acknowledge = forwardMessage(messagePacket);
            HashMap<String,String> replica = acknowledge.getData();
            for (Map.Entry<String, String> entry : replica.entrySet()) {
                String key = entry.getKey();
                if(checkNode(key).equals(myID)){
                    ContentValues keyValueToInsert = new ContentValues();
                    keyValueToInsert.put(KEY_FIELD, key);
                    keyValueToInsert.put(VALUE_FIELD,entry.getValue());
                    Variables.uri= DynamoUtilities.insert_self(Variables.uri,keyValueToInsert);
                }
            }
        } catch (SocketTimeoutException e) {
            Log.e("onrecover","!!!!!!!!!");
            // e.printStackTrace();
        }


        Log.e("recovered","at port "+myPort);
    }*/


   /* if(myID.equals(ringList.get(0))){
        first=true;
        Log.e("init", "im first node");

    }

    if(myID.equals(ringList.get(N-1))){
        last=true;
        Log.e("init","im last node");
    }*/


    //inside delete
            /* MessagePacket messagePacket = new MessagePacket();
            String ownerKey = checkNode(selection);
            String coordinator= idToPortMap.get(ownerKey);
            messagePacket.setOwner(coordinator);
            messagePacket.setKey(selection);
            String succ1key= succesorMap.get(ownerKey);
            String succ2key = succesorMap.get(succ1key);
            messagePacket.setSucc1(idToPortMap.get(succ1key));
            messagePacket.setSucc2(idToPortMap.get(succ2key));

            if(!coordinator.equals(myPort)){
                Log.e("D TEST", "NOT INSIDE MY DB");
                messagePacket.setType(DELETE);
                //send to all three
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
                return 0;
            }else{
                Log.e("D TEST", "INSIDE MY DB");
                int rows= DynamoUtilities.delete_self(selection);
                messagePacket.setType(DELETE_REP);
                //send to replicas
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
                return rows;
            }*/


    /*public Uri tempinsert(Uri uri, ContentValues values) {

        //check node
        //create message
        //if im owner insert self and fwd to succ1
        //if succ1 failed fwd to succ2
        //after recieving ack return uri

        //if im not owner send to owner
        //if owner failed
        //send to succ1
        //if im succ1 insert self and fwd tosucc2
        // return uri
        //else
        //after recieving ack return uri
        Log.e("INSERT","IN Insert of" +myPort);
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
        Acknowledge acknowledge= null;
        if(coordinator.equals(myPort)){
            //INSERT HERE
            Log.e("INSERT","I'm owner," +coordinator+"myport"+myPort);
            Uri newUri = insert_self(uri, values);
            messagePacket.setType(REPLICA1);
            messagePacket.setSendTo(messagePacket.getSucc1());
            Log.e("I_R","fwd to succ1 "+messagePacket.getSucc1());
                try {
                    acknowledge=forwardMessage(messagePacket);
                    Log.e("I_R","replicated in both the succ");
                } catch (SocketTimeoutException e) {
                    Log.e("INSERT","time out for first replica "+messagePacket.getSendTo());
                    messagePacket.setType(REPLICA2);
                    messagePacket.setSendTo(messagePacket.getSucc2());
                    Log.e("I_R","fwd to succ2 "+messagePacket.getSendTo());
                    try {
                        acknowledge=forwardMessage(messagePacket);
                        Log.e("ACK","received from replica1");
                    } catch (SocketTimeoutException e1) {
                        Log.e("INSERT","never print this");
                        Log.e("INSERT","time out for second replica "+messagePacket.getSendTo());
                        //e1.printStackTrace();
                    }
                    //e.printStackTrace();
                }
                return newUri;
        }else{
            Log.e("Insert","im not the owner, send to "+messagePacket.getOwner());
            messagePacket.setType(INSERT);
            messagePacket.setSendTo(messagePacket.getOwner());
            try {
                acknowledge=forwardMessage(messagePacket);
                Log.e("ACK","received from actual owner");
            } catch (SocketTimeoutException e) {
                Log.e("Insert","owner failed, send to succ1 "+messagePacket.getSucc1());
                messagePacket.setType(REPLICA1);
                messagePacket.setSendTo(messagePacket.getSucc1());
                if(messagePacket.getSucc1().equals(myPort)){
                    Log.e("INSERT","im the successor of the failed owner");
                    Uri newUri = insert_self(uri,values);
                        messagePacket.setType(REPLICA2);
                        messagePacket.setSendTo(messagePacket.getSucc2());
                        try {
                            acknowledge=forwardMessage(messagePacket);
                            Log.e("ACK","received from failed succ2");
                        } catch (SocketTimeoutException e1) {
                            Log.e("check","should not print this");
                            //e1.printStackTrace();
                        }
                    return newUri;
                    }else{
                    Log.e("INSERT","im not the successor of the failed owner, so send to "+messagePacket.getSucc1());
                    messagePacket.setType(REPLICA1);
                    messagePacket.setSendTo(messagePacket.getSucc1());
                    try {
                        acknowledge=forwardMessage(messagePacket);
                        Log.e("ACK","received from failed owner's succ");
                    } catch (SocketTimeoutException e1) {
                        Log.e("INSERT","should not print this");
                        //e1.printStackTrace();
                    }

                }

                //e.printStackTrace();
            }
        }
		return null;
	}*/


    /*public Cursor tempquery(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        //check owner
        //if im the owner
        //fwd q to succ2
          //if fail ask succ1
        //return cursor

        //if im not owner.
        //send to owner.
        //return cursor
        cobj=DynamoUtilities.query_self(LOCAL_INDICATOR);
        Log.e("check_q","initialize cursor "+cobj.getCount()+"");

        Acknowledge ack= null;
        if(selection.equals(GLOBAL_INDICATOR)){
                Log.e("QUERY", "INSIDE * QUERY");
                HashMap<String,String> global = DynamoUtilities.getDatafromCursor(cobj);
                MessagePacket messagePacket= new MessagePacket();
                for(String port:REMOTE_PORTS) {
                    if (!port.equals(myPort)) {
                        messagePacket.setSendTo(port);
                        messagePacket.setType(QUERY);
                        messagePacket.setKey(LOCAL_INDICATOR);
                        try {
                            ack = forwardMessage(messagePacket);
                            global.putAll(ack.getData());

                        } catch (SocketTimeoutException e) {
                            Log.e("QUERY", "query timed out at" + port);
                            e.printStackTrace();
                        }

                    }
                }
                MatrixCursor matrixCursor =DynamoUtilities.getCursorfromMap(global);
                return matrixCursor;
        }else if(selection.equals(LOCAL_INDICATOR)){
            return cobj;
        }else{
            Log.e("QUERY","call query with selection");
            String ownerKey = checkNode(selection);
            String coordinator= idToPortMap.get(ownerKey);
            Log.e("Q_SElect","owner port "+coordinator+"my port "+myPort);
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setOwner(coordinator);
            messagePacket.setKey(selection);
            String succ1key= succesorMap.get(ownerKey);
            String succ2key = succesorMap.get(succ1key);
            messagePacket.setSucc1(idToPortMap.get(succ1key));
            messagePacket.setSucc2(idToPortMap.get(succ2key));
            if(coordinator.equals(myPort)){
                Log.e("QUERY","im the owner, query succ2");
                //query suu2
                messagePacket.setType(QUERY_REPLICATE);
                messagePacket.setSendTo(messagePacket.getSucc2());
                try {
                    Log.e("QUERY","fwd query from owner"+messagePacket.getOwner()+"to succ2"+messagePacket.getSendTo());
                    ack=forwardMessage(messagePacket);
                    Log.e("QUERY","got from succ2");
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                    matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                    return matrixCursor;
                } catch (SocketTimeoutException e) {
                    Log.e("QUERY","time out at succ2"+messagePacket.getSucc2());
                    messagePacket.setSendTo(messagePacket.getSucc1());
                    try {
                        Log.e("QUERY","fwd to succ1 coz 2 failed "+messagePacket.getSucc2()+messagePacket.getSendTo());
                        messagePacket.setType(QUERY_REPLICATE);
                        ack=forwardMessage(messagePacket);
                        Log.e("QUERY","got from succ1 because succ2 failed");
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                        matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                        return matrixCursor;
                    } catch (SocketTimeoutException e1) {
                        Log.e("QUERY","!!!!!!!!!");
                        return null;
                    }
                }
            }else{
                Log.e("QUERY","im not thw owner fwd query call to owner "+messagePacket.getOwner());
                messagePacket.setType(QUERY);
                messagePacket.setSendTo(messagePacket.getOwner());
                try {
                    ack=forwardMessage(messagePacket);
                    Log.e("QUERY","got from actual owner");
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                    matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                    return matrixCursor;
                } catch (SocketTimeoutException e) {
                    Log.e("QUERY","timeout at coordinator");
                    if(myPort.equals(messagePacket.getSucc2())){
                        Log.e("Q_timeout","query myself because im second succersor");
                        cobj=query_self(selection);
                        return cobj;
                    }else{
                        messagePacket.setType(QUERY_REPLICATE);
                        messagePacket.setSendTo(messagePacket.getSucc2());
                        try {
                            ack=forwardMessage(messagePacket);
                            Log.e("QUERY","got from succ2 of the failed owner");
                                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
                                matrixCursor.addRow(new String[]{selection, ack.getData().get(selection)});
                                return matrixCursor;

                        } catch (SocketTimeoutException e1) {
                            Log.e("QUERY","should not print this");
                        }
                    }
                }
            }
        }
		return cobj;
	}*/


    /*if (type.equals(Constants.INSERT)){
                //insert
                //return ack
                Log.e("I","insert in owner "+incoming.getOwner()+" myport"+myPort);
                Variables.uri=callInsert(incoming);
                Acknowledge ack =new Acknowledge();
                Log.e("I","send ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if (type.equals(Constants.REPLICA1)){
                //can be recieved from actual owner or initiator
                //insert self and fwd with half timeout
                //return ack
                Log.e("IR1","insert in succ1 "+incoming.getSucc1()+" myport"+myPort);
                Variables.uri=callInsertSelf(incoming);
                incoming.setType(Constants.REPLICA2);
                Log.e("IR1","fwd to succ2 "+incoming.getSucc2()+" my succ"+Variables.succ1);
                incoming.setSendTo(incoming.getSucc2());
                Acknowledge ack =new Acknowledge();
                try {
                    ack = forwardMessage(incoming);
                    Log.e("IR1S","succesfully inserted at succ2");
                } catch (SocketTimeoutException e) {
                    Log.e("timeout","timeout at succ2 insert");
                }
                Log.e("IR1","send ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else if (type.equals(Constants.REPLICA2)){
                // can be recieved by either owner or succ1
                //insert self
                //return ack
                Log.e("IR2","insert in succ2 "+incoming.getSucc2()+" myport"+myPort+"from "+incoming.getSucc1()+" or"+incoming.getOwner()+"my pred "+prev1);
                Variables.uri=callInsertSelf(incoming);
                Log.e("IR2","send ack");
                Acknowledge ack =new Acknowledge();
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if(type.equals(Constants.QUERY)){
                //im the owner.. call query
                //return ack
                Log.e("QOwner","message owner"+incoming.getOwner()+" myport"+myPort);
                Cursor cobj= callQuery(incoming);
                Acknowledge ack = new Acknowledge();
                ack.setData(DynamoUtilities.getDatafromCursor(cobj));
                Log.e("QOwner","send ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else if(type.equals(QUERY_REPLICATE)){
                //im not the owner,
                //received either from owner or originator or succ2
                //call queryself
                // return ack
                Log.e("QR","message owner"+incoming.getOwner()+" myport"+myPort);
                if(incoming.getSucc2().equals(myPort))
                    Log.e("QR","im succ2");
                else
                    Log.e("QR","im succ1 "+incoming.getSucc1()+"myport "+myPort);

                Cursor cursor = callQuerySelf(incoming);
                Acknowledge ack = new Acknowledge();
                ack.setData(DynamoUtilities.getDatafromCursor(cursor));
                Log.e("QR","send ack");
                try {
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    out.writeObject(ack);
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else */





    /*private Acknowledge forwardMessage(MessagePacket messagePacket) throws SocketTimeoutException {
        // create socket and send message
        Log.e("FWD","SEND FROM" +myPort);
        Acknowledge acknowledge=null;

            Log.e("FWD","SEND REPLICA to" +messagePacket.getSendTo());
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messagePacket.getSendTo()));
                socket.setSoTimeout(2000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                messagePacket.setSocket(null);
                out.writeObject(messagePacket);
                Log.e("FWD","WAITING for ACK");
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                acknowledge = (Acknowledge) in.readObject();

            } catch (SocketTimeoutException e) {
                Log.e("FWD","timeout exception... was waiting for"+messagePacket.getSendTo());
                throw new SocketTimeoutException();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (OptionalDataException e) {
                e.printStackTrace();
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (StreamCorruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return acknowledge;

    }
*/
}
