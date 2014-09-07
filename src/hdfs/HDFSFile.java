package hdfs;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import hdfs.HDFSCommon;
import hdfs.DataNodeInfo;

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
    /** ?? commit time */
    private Date commitTime = null;

    
    /**
     * Constructor
     * @param name file name
     */
    public HDFSFile(String name) {
        this.name = name;
        this.chunkList = new ArrayList<HDFSChunk>();
        this.replicaFactor = HDFSCommon.DFT_REPLICAS;
    }
    

    /**
     * add a chunk to the file
     * @param index chunk index in this file
     */
    public void addChunk(HDFSChunk chunk) {
        this.chunkList.add(chunk);
    }

    /**
     * remove HDFS chunk from the file by object
     * @param index chunk index in this file
     */
    public void removeChunk(HDFSChunk chunk) {
        this.chunkList.remove(chunk);
    }
    
    /**
     * overload method : remove HDFS chunk from the file by index
     * @param index chunk index in this file
     */
    public void removeChunk(int index) {
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
            return chunkList.get(index);
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

