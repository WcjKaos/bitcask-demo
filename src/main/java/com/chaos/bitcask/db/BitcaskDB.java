package com.chaos.bitcask.db;

import com.chaos.bitcask.constant.Constant;
import com.chaos.bitcask.entity.Entry;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Data
public class BitcaskDB {

    private HashMap<String, Integer> indexes;
    private DBFile dbFile;
    private String dbPath;

    private BitcaskDB() {
    }

    public static BitcaskDB open(String dbPath) throws IOException {
        BitcaskDB bitcaskDB = new BitcaskDB();
        File file = new File(dbPath);
        if (!file.exists()) file.mkdirs();

        DBFile db = DBFile.createDB(dbPath);
        bitcaskDB.setDbFile(db);
        bitcaskDB.setIndexes(new HashMap<>());
        bitcaskDB.setDbPath(dbPath);
        bitcaskDB.loadIndexesFromFile();
        return bitcaskDB;
    }

    public void merge() throws IOException {
        DBFile originFile = this.dbFile;
        if (originFile.getFileLength() == 0) return;

        List<Entry> entries = new LinkedList<>();
        int offset = 0;

        while (true) {
            Entry entry = originFile.read(offset);
            if (entry == null) {
                break;
            }
            Integer keyOff = this.indexes.get(new String(entry.getKey()));
            //hash表中的数据为有效的
            if (keyOff != null && keyOff == offset) entries.add(entry);
            offset += entry.getSize();
        }

            synchronized (BitcaskDB.class) {
                DBFile mergeFile = DBFile.createMergeFile("temp/db");
                if (!entries.isEmpty()) {
                    for (Entry entry : entries) {
                        int mergeOffset = mergeFile.getFileLength();
                        mergeFile.write(entry);
                        this.indexes.put(new String(entry.getKey()), mergeOffset);
                    }
                }
                //将临时文件复制到数据文件
                Files.copy(mergeFile.getDbFile().toPath(), originFile.getDbFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
                originFile.setFileLength((int) originFile.getDbFile().length());
                mergeFile.getDbFile().delete();
        }
    }

    public void loadIndexesFromFile() {
        if (this.dbFile == null) return;

        int offset = 0;
        while (true) {
            Entry read = this.dbFile.read(offset);
            if (read == null) break;

            this.indexes.put(new String(read.getKey()), offset);

            if (read.getMark() == Constant.DEL) {
                this.indexes.remove(new String(read.getKey()));
            }

            offset += read.getSize();
        }
    }


    public void put(String key, String value) {
        if (key == null || key.isBlank() || key.isEmpty()) return;
        synchronized (BitcaskDB.class) {
            Integer offset = this.dbFile.getFileLength();
            Entry entry = new Entry(key.getBytes(), value.getBytes(), Constant.PUT);
            this.dbFile.write(entry);
            this.indexes.put(key, offset);
        }
    }

    public String get(String key) {
        if (key == null || key.isBlank() || key.isEmpty()) return null;

        synchronized (BitcaskDB.class) {
            Integer offset = this.indexes.get(key);
            if (offset != null) {
                Entry entry = this.dbFile.read(offset);
                if (entry != null) return new String(entry.getValue());
            }
        }
        return null;
    }

    public void del(String key) {
        Integer integer = this.indexes.get(key);
        if (integer == null) return;
        Entry entry = new Entry(key.getBytes(), new byte[0], Constant.DEL);
        this.dbFile.write(entry);
        this.indexes.remove(key);
    }


}
