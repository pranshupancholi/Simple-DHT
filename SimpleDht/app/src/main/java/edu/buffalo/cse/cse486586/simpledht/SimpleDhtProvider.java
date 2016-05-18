package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map.Entry;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    HashMap<String, String> dht = new HashMap<String, String>();
    HashMap<String, String> ret_data = new HashMap<String, String>();
    String thisNodeId;
    String predNodeId;
    int thisNode;
    int predNode;
    int succNode;
    boolean flag = false;

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        thisNode = Integer.parseInt(portStr) * 2;

        thisNodeId = returnHash(portStr);
        predNodeId = thisNodeId;
        //Log.d(TAG, "Hash Value of "+thisNode+" : "+thisNodeId);
        //Log.d(TAG, "thisport before join "+thisNode);
        Message msg = new Message(11108, thisNodeId, null, Message.MType.JOIN, thisNode, thisNode, null);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "ServerSocket Creation Exception " + e.getMessage());
            return false;
        }
        predNode = thisNode;
        succNode = thisNode;
        return true;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //Log.d(TAG, "Insert Main Called");
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashKey = returnHash(key);
        if (inMiddle(hashKey)) {
            Log.d(TAG, "Inserting to hash map key " + key);
            dht.put(key, value);
        } else {
            Log.d(TAG, "Fwding Insert request to succ " + succNode);
            Message msg = new Message(succNode, key, value, Message.MType.INSERT, 0, 0, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
        return uri;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        //Log.d(TAG, "Delete Main Called");
        if (selection.equals("@")) {
            dht.clear();
        } else if (selection.equals("*")) {
            dht.clear();
            Message msg = new Message(succNode, selection, null, Message.MType.DELETE, 0, 0, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        } else {
            if (inMiddle(returnHash(selection))) {
                dht.remove(selection);
            } else {
                Message msg = new Message(succNode, selection, null, Message.MType.DELETE, 0, 0, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        if (selection.equals("@")) {
            for (Entry<String, String> entry : dht.entrySet()) {
                cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }
        } else if (selection.equals("*")) {
            Message msg = new Message(succNode, selection, null, Message.MType.QUERY, thisNode, 0, dht);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            while (!flag) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Log.d(TAG, "InterruptedException " + e.getMessage());
                }
            }
            flag = false;
            for (Entry<String, String> entry : ret_data.entrySet()) {
                cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }
            ret_data = null;
        } else {
            if (inMiddle(returnHash(selection))) {
                //Log.d(TAG, "VALUE FOUND for "+selection);
                cursor.addRow(new String[]{selection, dht.get(selection)});
            } else {
                Log.d(TAG, "Query Forwarded");
                Message msg = new Message(succNode, selection, null, Message.MType.QUERY, thisNode, 0, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                while (!ret_data.containsKey(selection)) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Log.d(TAG, "InterruptedException " + e.getMessage());
                    }
                }
                //Log.d(TAG, "Key Ret :"+selection+" Value Ret "+ret_data.get(selection));
                cursor.addRow(new String[]{selection, ret_data.get(selection)});
                ret_data.remove(selection);
            }
        }
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Log.d(TAG, "Inside Server");
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objectInputStream =
                            new ObjectInputStream(socket.getInputStream());
                    Message message = (Message) objectInputStream.readObject();
                    //Log.d(TAG, "Server Cur port " + thisNode + " Pred port " + predNode + " " + "Succ Port" + succNode);
                    //Log.d(TAG, "Server Message MType " + message.type + " from Predecessor Port" + message.pred + " Succesor Port " + message.succ);
                    if (message.type == Message.MType.JOIN) {
                        //Log.d(TAG, "Inside Join");
                        //Log.d(TAG, "Join requested to " + thisNode + " Predecessor port " + predNode + " " + "Succesor Port" + succNode);
                        //Log.d(TAG, "Join requested from MType " + message.type + " Pred Port" + message.pred + " Succ Port " + message.succ);
                        if (inMiddle(message.key)) {
                            //Log.d(TAG, "Inside If Join ");
                            Message msg = new Message(predNode, message.key, null, Message.MType.UPD_SUCC, 0, message.succ, null);
                            invokeNewClientTask(msg);
                            Message msg1 = new Message(message.pred, predNodeId, thisNodeId, Message.MType.UPD_PRED_SUCC, predNode, thisNode, null);
                            invokeNewClientTask(msg1);
                            predNodeId = message.key;
                            predNode = message.pred;
                        } else {
                            //Log.d(TAG, "Inside else 2 Join");
                            Message msg = new Message(succNode, message.key, message.value, message.type, message.pred, message.succ, null);
                            invokeNewClientTask(msg);
                        }
                    } else if (message.type == Message.MType.UPD_PRED_SUCC) {
                        predNodeId = message.key;
                        predNode = message.pred;
                        succNode = message.succ;
                        //Log.d(TAG, "Inside Accept updated P: " + predNode + " S: " + succNode);
                    } else if (message.type == Message.MType.UPD_SUCC) {
                        succNode = message.succ;
                        //Log.d(TAG, "Inside Set Succ " + succNode);
                    } else if (message.type == Message.MType.INSERT) {
                        if (inMiddle(returnHash(message.key))) {
                            //Log.d(TAG, "Server Inserting to hash map key "+message.key);
                            dht.put(message.key, message.value);
                        } else {
                            //Log.d(TAG, "Server Fwding Insert request to succ " + succNode);
                            Message msg = new Message(succNode, message.key, message.value, Message.MType.INSERT, 0, 0, null);
                            invokeNewClientTask(msg);
                        }
                    } else if (message.type == Message.MType.DELETE) {
                        if (message.key.equals("@")) {
                            dht.clear();
                        } else if (message.key.equals("*")) {
                            dht.clear();
                            Message msg = new Message(succNode, message.key, null, Message.MType.DELETE, 0, 0, null);
                            invokeNewClientTask(msg);
                        } else {
                            if (inMiddle(returnHash(message.key))) {
                                dht.remove(message.key);
                            } else {
                                Message msg = new Message(succNode, message.key, null, Message.MType.DELETE, 0, 0, null);
                                invokeNewClientTask(msg);
                            }
                        }
                    } else if (message.type == Message.MType.QUERY) {
                        if (message.key.equals("*")) {
                            //if(dht == null)
                            //Log.d(TAG, "sMap is null");
                            for (Entry<String, String> entry : dht.entrySet()) {
                                //Log.d(TAG, "key "+entry.getKey()+" Value "+entry.getValue());
                                message.map.put(entry.getKey(), entry.getValue());
                            }
                            if (message.pred == thisNode) {
                                ret_data.clear();
                                ret_data.putAll(message.map);
                                flag = true;
                            } else {
                                message.fwdToPort = succNode;
                                invokeNewClientTask(message);
                            }
                        } else if (inMiddle(returnHash(message.key))) {
                            //Log.d(TAG, "Inside QUERY ELSE IF");
                            Message msg = new Message(message.pred, message.key, dht.get(message.key), Message.MType.RET_MSG, 0, 0, null);
                            invokeNewClientTask(msg);
                        } else {
                            //Log.d(TAG, "Inside QUERY ELSE");
                            message.fwdToPort = succNode;
                            invokeNewClientTask(message);
                        }
                    } else if (message.type == Message.MType.RET_MSG) {
                        ret_data.put(message.key, message.value);
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Server IO Read error: " + e.getMessage());
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "Server ClassNotFoundException " + e.getMessage());
                e.printStackTrace();
            }
            return null;
        }
    }

    public void invokeNewClientTask(Message msg) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    public String returnHash(String key) {
        String hashKey = null;
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash value generation exception " + e.getMessage());
        }
        return hashKey;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean inMiddle(String key) {
        //Log.d(TAG, "mPredId "+predNodeId);
        //Log.d(TAG, "mNodeId "+predNodeId);
        //Log.d(TAG, "key "+key);
        if (thisNodeId.compareTo(predNodeId) == 0) {
            //Log.d(TAG, "Only 1 node in the ring");
            //Log.d(TAG, "RET 1");
            return true;
        }
        if ((key.compareTo(predNodeId) > 0) && (key.compareTo(thisNodeId) < 0) && (predNodeId.compareTo(thisNodeId) < 0)) {
            //Log.d(TAG, "RET 2");
            return true;
        } else if ((key.compareTo(predNodeId) > 0) && (key.compareTo(thisNodeId) > 0) && (predNodeId.compareTo(thisNodeId) > 0)) {
            //Log.d(TAG, "RET 3");
            return true;
        } else if ((key.compareTo(predNodeId) < 0) && (key.compareTo(thisNodeId) < 0) && (thisNodeId.compareTo(predNodeId) < 0)) {
            //Log.d(TAG, "RET 4");
            return true;
        }
        return false;
    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            try {
                Message msg = msgs[0];
                //Log.d(TAG, "Inside Client sent "+msg.type+" to "+msg.fwdToPort+" Predecessor is: "+msg.pred+" Successor is :"+msg.succ);
                Socket socket =
                        new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.fwdToPort);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                        socket.getOutputStream());
                objectOutputStream.writeObject(msg);
                objectOutputStream.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException" + e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + e.getMessage());
            }
            return null;
        }
    }
}