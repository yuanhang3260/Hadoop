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
        super(name, size);
        chunkTable = new ConcurrentHashMap<Integer, HDFSChunk>();
    }
    
    /** get chunkTable */
    public ConcurrentHashMap<Integer, HDFSChunk> getChunkTable() {
        return this.chunkTable;
    }

    /**
     * add a chunk to the file
     * @param index chunk index in this file
     */
    public void addChunk(HDFSChunk chunk) {
        this.chunkTable.put(chunk.getChunkNum(), chunk);
        this.setSize(this.getSize() + chunk.getChunkSize());
    }
    
    /**
     * overload method : remove HDFS chunk from the file by index
     * @param index chunk index in this file
     */
    public void removeChunk(int chunkNum) {
        this.chunkTable.remove(chunkNum);
    }

}

