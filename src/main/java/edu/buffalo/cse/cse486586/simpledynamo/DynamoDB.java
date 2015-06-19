package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import edu.buffalo.cse.cse486586.globals.Constants;

/**
 * Created by archana on 4/13/15.
 */
public class DynamoDB extends SQLiteOpenHelper {
    public DynamoDB(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE "+ Constants.TABLE_NAME+"("+Constants.KEY_FIELD+" TEXT UNIQUE, "+Constants.VALUE_FIELD+" TEXT NOT NULL);");
        //db.execSQL("CREATE TABLE "+ Constants.TABLE_NAME+"("+Constants.KEY_FIELD+" TEXT PRIMARY KEY, "+Constants.VALUE_FIELD+" TEXT NOT NULL, "+Constants.VERSION_FIELD+"NUMBER);");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS "+Constants.TABLE_NAME);
        onCreate(db);

    }
}
