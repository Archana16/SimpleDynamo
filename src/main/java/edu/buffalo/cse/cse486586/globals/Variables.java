package edu.buffalo.cse.cse486586.globals;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;

import java.util.ArrayList;
import java.util.HashMap;


import static edu.buffalo.cse.cse486586.utilities.DynamoUtilities.buildUri;

/**
 * Created by archana on 4/13/15.
 */
public class Variables {
    public static Context context = null;
    public static Cursor cobj= null;
    public static String portStr= null;
    public static String myPort=null ;
    public static String myID=null;
    public static String succ1 = null;
    public static String succ2 = null;
    public static String prev1 = null;
    public static String prev2 = null;
    public static SQLiteDatabase dynamoDB=null;
    public static boolean first= false;
    public static boolean last= false;
    public static boolean loading= true;



    public static HashMap<String,String> succesorMap = new HashMap<String,String>();
    public static HashMap<String,String> predecessorMap = new HashMap<String,String>();
    public static ArrayList<String> ringList = new ArrayList<String>();
    public static HashMap<String,String> idToPortMap = new HashMap<String,String>();
    public static Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
}
