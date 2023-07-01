package com.chaos.bitcask.util;

import com.chaos.bitcask.entity.Entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

public class Util {

    public static File createFile(String path,String fileName) throws IOException {
        String fullPath = Paths.get(path, fileName).toString();
        File file = new File(fullPath);
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }


    public static byte[] encode(Entry entry){
        ByteBuffer buf = ByteBuffer.allocate(entry.getSize());
        buf.putInt(entry.getKeySize());
        buf.putInt(entry.getValueSize());
        buf.putShort(entry.getMark());
        buf.put(entry.getKey());
        buf.put(entry.getValue());
        return buf.array();

    }

    public static Entry decode(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        int ks = byteBuffer.getInt();
        int vs = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        return new Entry(ks, vs, mark); // Modify this line to include actual key and value
    }
}
