package edu.buffalo.cse.cse486586.utilities;

import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static edu.buffalo.cse.cse486586.globals.Constants.*;
import static edu.buffalo.cse.cse486586.globals.Constants.LOCAL_INDICATOR;
import static edu.buffalo.cse.cse486586.globals.Constants.REMOTE_PORTS;
import static edu.buffalo.cse.cse486586.globals.Constants.TABLE_NAME;
import static edu.buffalo.cse.cse486586.globals.Variables.*;

/**
 * Created by archana on 4/13/15.
 */
public class DynamoUtilities {
    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static void createRingAndMap(){
        String key =null;
        for(int i=0; i<5;i++){
            try {
                key=genHash(PORTS[i]);
                ringList.add(key);
                idToPortMap.put(key,REMOTE_PORTS[i]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        Collections.sort(ringList);
        Log.e("Created ring",ringList.toString());
        Log.e("Created mapping",idToPortMap.toString());
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    public static String checkNode(String key) {
        Log.e("checkNode", "check for "+key);
        String coordinator = null;
        String hashKey = null;
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(hashKey.compareTo(ringList.get(ringList.size()-1))>0 || hashKey.compareTo(ringList.get(0))<0){
            //Log.e("checkNode", "first node");
            coordinator = ringList.get(0);
        }else{
            for(int i = 0; i < ringList.size()-1;i++){
                if(hashKey.compareTo(ringList.get(i))>0  && hashKey.compareTo(ringList.get(i+1))<0){
                    coordinator = ringList.get(i+1);
                   // Log.e("checkNode", "actual coordinate "+coordinator);
                    break;
                }
            }
        }

        Log.e("checkNode exit", "coordinator "+idToPortMap.get(coordinator));
        return coordinator;
    }

    public static void UpdateMaps(){
        //first node
        succesorMap.put(ringList.get(0), ringList.get(1));
        predecessorMap.put(ringList.get(0),ringList.get(ringList.size()-1));
        //last node
        succesorMap.put(ringList.get(ringList.size()-1),ringList.get(0));
        predecessorMap.put(ringList.get(ringList.size() - 1), ringList.get(ringList.size()-2));
        for(int i=1; i <=ringList.size()-2;i++ ){
            succesorMap.put(ringList.get(i),ringList.get(i+1));
            predecessorMap.put(ringList.get(i),ringList.get(i - 1));
        }
        Log.e("Created succ map",succesorMap.toString());
        Log.e("Created pred map",predecessorMap.toString());

    }


    public static Cursor query_self(String selection){
        Log.e("Q_SELF","INSIDE QUERY");
        String query = null;
        Cursor cursor =null;
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables(TABLE_NAME);
        if(selection.equals(LOCAL_INDICATOR)){
            Log.e("Q_SELF","selection "+selection+" indicator"+LOCAL_INDICATOR);
            query = "SELECT  * FROM " + TABLE_NAME;
        }else{
            Log.e("Q_SELF","selection "+selection);
            query = "SELECT  * FROM " + TABLE_NAME+ " WHERE key='"+selection+"'";
        }
        cursor = dynamoDB.rawQuery(query, null);
        cursor.setNotificationUri(context.getContentResolver(), uri);
        Log.v("query", "queried local db");
        if(cursor==null)
            Log.e("Q_SELF","cursor null");
        return cursor;
    }
    public static synchronized Uri insert_self(Uri uri, ContentValues values){
        Log.e("Icheck","IN Insert of" +myPort);
        long row = dynamoDB.insertWithOnConflict(
                TABLE_NAME,
                null,
                values, SQLiteDatabase.CONFLICT_REPLACE);
        if(row >0){
            Log.v("insert", values.toString());
            Uri newUri = ContentUris.withAppendedId(uri, row);
            context.getContentResolver().notifyChange(newUri, null);
            return newUri;
        }else{
            Log.v("insert", "failed");
            return uri;
        }
    }

    public static int delete_self(String selection){
        int rows=0;
        Log.e("D_SELF","INSIDE DELETE");

        if(selection.equals(LOCAL_INDICATOR)){
            Log.e("D_SELF","selection "+selection+" indicator"+LOCAL_INDICATOR);
            rows = dynamoDB.delete(TABLE_NAME, null, null);
        }else{
            Log.e("D_SELF","selection "+selection);
            String where=KEY_FIELD+"='"+selection+"'";
            rows= dynamoDB.delete(TABLE_NAME,where,null);
        }
        return rows;
    }

    public static HashMap<String,String> getDatafromCursor(Cursor obj) /*throws Exception*/{
        HashMap<String, String> result = new HashMap<String, String>();
        if(obj.getCount() !=0) {
            Log.e("convert",obj.getCount()+"");

            int keyIndex = obj.getColumnIndex(KEY_FIELD);
            int valueIndex = obj.getColumnIndex(VALUE_FIELD);
       /* if (keyIndex == -1 || valueIndex == -1) {
            Log.e("error", "Wrong columns");
            obj.close();
            throw new Exception();
        }*/
            obj.moveToFirst();
        /*if (!(obj.isFirst() && obj.isLast())) {
            Log.e("error", "Wrong number of rows");
            obj.close();
            throw new Exception();
        }*/
            String returnKey = null;
            String returnValue = null;
            while (!obj.isLast()) {
                returnKey = obj.getString(keyIndex);
                returnValue = obj.getString(valueIndex);
                result.put(returnKey, returnValue);
                obj.moveToNext();
            }
            //add the last entry
            returnKey = obj.getString(keyIndex);
            returnValue = obj.getString(valueIndex);
            result.put(returnKey, returnValue);
            Log.e("convert",result.toString());
            return result;
        }
        Log.e("convert","empty");
        return result;
    }

    public static MatrixCursor getCursorfromMap(HashMap<String, String> global) {


        MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
        Iterator it = global.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Log.e("key",entry.getKey().toString());

            matrixCursor.addRow(new String[]{entry.getKey().toString(), entry.getValue().toString()});

        }

        Log.e("MAtrix",matrixCursor.toString());
        return matrixCursor;
    }



    public static boolean checkDB() {
        SQLiteDatabase checkDB = null;
        try {
            Log.e("DB","check if already created");
            checkDB = SQLiteDatabase.openDatabase(DB_PATH, null,
                    SQLiteDatabase.OPEN_READONLY);

            checkDB.close();
            Log.e("DB","created");
            return true;
        } catch (SQLiteException e) {
            Log.e("DB","not created");
            return false;
        }
    }
}
