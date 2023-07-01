package com.chaos.bitcask.entity;

import com.chaos.bitcask.constant.Constant;
import lombok.Data;

@Data
public class Entry {

    private byte[] key;

    private byte[] value;

    private int keySize;

    private int valueSize;

    private short mark;

    public Entry(byte[] key, byte[] value, short mark) {
        this.key = key;
        this.value = value;
        this.mark = mark;
        this.keySize = key.length;
        this.valueSize = value.length;
    }

    public Entry(int keySize, int valueSize, short mark) {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.mark = mark;
    }

    public int getSize(){
        return this.keySize+this.valueSize+ Constant.ENTRY_HEADER_SIZE;
    }
}
