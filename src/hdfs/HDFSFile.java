package hdfs;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import hdfs.HDFSCommon;
import hdfs.DataNodeInfo;
import hdfs.HDFSFileMeta;

import hdfs.HDFSFileMeta;

/**
 * An abstraction of HDFS files
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSFile extends HDFSFileMeta implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -6302186159396021997L;
    /** chunk table maps chunkNum -> chunk object*/
    private ConcurrentHashMap<Integer, HDFSChunk> chunkTable;

    /**
     * Constructor
     * @param name file name
     */
    public HDFSFile(String name, int size) {
        this.super(name, size);
        chunkList = new ConcurrentHashMap<Integer, HDFSChunk>();
    }
    
    /** get chunkList */
    public ConcurrentHashMap<Integer, HDFSChunk> getChunkTable() {
        return this.chunkList;
    }

    /**
     * add a chunk to the file
     * @param index chunk index in this file
     */
    public void addChunk(HDFSChunk chunk) {
        this.chunkList.put(chunk.getChunkNum(), chunk);
    }
    
    /**
     * overload method : remove HDFS chunk from the file by index
     * @param index chunk index in this file
     */
    public void removeChunk(int chunkNum) {
        this.chunkList.remove(chunkNum);
    }

}

