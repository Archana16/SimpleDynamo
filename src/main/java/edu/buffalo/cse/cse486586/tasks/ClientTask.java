package edu.buffalo.cse.cse486586.tasks;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import edu.buffalo.cse.cse486586.globals.Constants;
import edu.buffalo.cse.cse486586.globals.MessagePacket;

import static edu.buffalo.cse.cse486586.globals.Constants.REMOTE_PORTS;

/**
 * Created by archana on 4/13/15.
 */
public class ClientTask extends AsyncTask<MessagePacket, Void, Void> {



    @Override
    protected Void doInBackground(MessagePacket... params) {
        MessagePacket messagePacket = params[0];
        Log.e("CLIENT", "NEW CLIENT");
        for(String port : REMOTE_PORTS) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(messagePacket);
                out.close();
                socket.close();

            }catch(IOException e){
                Log.e("client exception ",e.getMessage());
            }

        }
        return null;
    }
}
