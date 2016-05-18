package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

public class Message implements Serializable {
    String key;
    String value;
    int pred;
    int succ;
    MType type;
    int fwdToPort;
    HashMap<String, String> map = null;

    public Message(){

    }

    public Message(int fwdP, String ky, String val, MType typ, int pre, int suc, HashMap<String, String> m) {
        fwdToPort = fwdP;
        key = ky;
        value = val;
        pred = pre;
        succ = suc;
        type = typ;
        map = m;
    }
    enum MType{
        JOIN,
        UPD_PRED_SUCC,
        UPD_SUCC,
        INSERT,
        DELETE,
        QUERY,
        RET_MSG
    }
}