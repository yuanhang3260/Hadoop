package hdfs;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import hdfs.HDFSCommon;
import hdfs.DataNodeInfo;
import hdfs.NameNodeInterface;

/**
 * An abstraction of HDFS files
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSFile implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -6302186159396021997L;
    /** file name */
    private String name;
    /** chunk list */
    private ArrayList<HDFSChunk> chunkList;
    /** ?? replica factor */
    private int replicaFactor;
    /** OutputStream */
    private HDFSOutputStream newOutputStream = null;
    /** ?? commit time */
    private Date commitTime = null;

    
    /**
     * Constructor
     * @param name file name
     */
    public HDFSFile(String name) {
        this.name = name;
        this.chunkList = new ArrayList<HDFSChunk>();
        this.replicaFactor = DFT_REPLICAS;
    }
    

    /**
     * remove HDFS chunk from the file
     * @param index chunk index in this file
     */
    public void addChunk(HDFSChunk chunk) {
        this.chunkList.add(chunk);
    }

    /**
     * remove HDFS chunk from the file
     * @param index chunk index in this file
     */
    public void removeChunk(int index) {
        // TODO delete the chunk file from dataNode ?
        this.chunkList.remove(index);
    }
    
    
    /**
     * search for chunk by index
     * @param index Chunk index in this file
     */
    public HDFSChunk getChunkByIndex(int index) {
        if (index < 0 || index >= chunkList.size()) {
            return null;
        }
        else {
            return chunkList.get(index) {
        }
    }
        
    /**
     * get chunkList
     */
    public ArrayList<HDFSChunk> getChunkList() {
        return this.chunkList;
    }
    
    /**
     * get file name
     */
    public String getName() {
        return this.name;
    }

    /**
     * ?? get ReplicaFactor
     */
    public int getReplicaFactor() {
        return this.replicaFactor;
    }
    
    /**
     * get Input Srream
     */
    public HDFSInputStream getInputStream() {
        return new HDFSInputStream(this.getChunkList());
    }
    
    /**
     * set commit time
     */
    public void setCommitTime(Date time) {
        this.commitTime = time;
    }
    
    /**
     * get commit time
     */
    public Date getCommitTime() {
        return this.commitTime;
    }
}

