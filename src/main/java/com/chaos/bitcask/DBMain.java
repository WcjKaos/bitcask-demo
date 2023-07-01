package com.chaos.bitcask;

import com.chaos.bitcask.db.BitcaskDB;

import java.io.IOException;

public class DBMain {
    public static void main(String[] args) throws IOException {
        BitcaskDB bitcaskDB = BitcaskDB.open("temp/db");
        bitcaskDB.put("key","value");
        String s1 = bitcaskDB.get("key");
        System.out.println(s1);
        bitcaskDB.del("key");
        String s2= bitcaskDB.get("key");
        System.out.println(s2);
        int fileLength = bitcaskDB.getDbFile().getFileLength();
        System.out.println(fileLength);
        bitcaskDB.merge();
        int mergeLenth = bitcaskDB.getDbFile().getFileLength();
        System.out.println(mergeLenth);
        String s3= bitcaskDB.get("key");
        System.out.println(s3);
    }
}
