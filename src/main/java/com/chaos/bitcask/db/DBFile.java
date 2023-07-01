package com.chaos.bitcask.db;

import com.chaos.bitcask.constant.Constant;
import com.chaos.bitcask.entity.Entry;
import com.chaos.bitcask.util.Util;
import lombok.Data;

import java.io.*;

@Data
public class DBFile {

    private File dbFile;

    private int fileLength;

    public static DBFile createDB(String path) throws IOException {
        DBFile dbFile = new DBFile();
        File file = Util.createFile(path, Constant.DB_FILE_NAME);
        dbFile.setDbFile(file);
        dbFile.setFileLength((int) file.length());
        return dbFile;
    }

    public static DBFile createMergeFile(String path) throws IOException {
        DBFile dbFile = new DBFile();
        File file = Util.createFile(path, Constant.MERGE_FILE_NAME);
        dbFile.setDbFile(file);
        dbFile.setFileLength((int) file.length());
        return dbFile;
    }

    public Entry read(int offset){
        if (offset>=this.fileLength) return null;
        Entry entry;
        try(RandomAccessFile file = new RandomAccessFile(this.dbFile,"rw")){
            byte[] buf = new byte[Constant.ENTRY_HEADER_SIZE];
            file.seek(offset);
            file.read(buf);
            entry = Util.decode(buf);

            offset += Constant.ENTRY_HEADER_SIZE;
            if (entry.getKeySize() > 0) {
                byte[] key = new byte[entry.getKeySize()];
                file.seek(offset);
                file.read(key);
                entry.setKey(key);
            }

            offset += entry.getKeySize();
            if (entry.getValueSize() > 0) {
                byte[] value = new byte[entry.getValueSize()];
                file.seek(offset);
                file.read(value);
                entry.setValue(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return entry;
    }

    public void write(Entry entry) {
        byte[] entryBuf = Util.encode(entry);
        try (RandomAccessFile file = new RandomAccessFile(this.dbFile,"rw")) {
            file.seek(this.fileLength);
            file.write(entryBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.fileLength += entry.getSize();
    }
}
