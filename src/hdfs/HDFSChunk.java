package hdfs;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import hdfs.HDFSCommon;
import hdfs.DataNodeInfo;

/** 
 * HDFS file chunk class
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSChunk implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -253895999225595586L;
    /** HDFS file name */
    private String fileName;
    /** chunk name = FileName_ChunkNum */
    private String chunkName;
    /** replica locations in dataNodes */
    private ArrayList<DataNodeInfo> locations;
    /** Chunk size */
    private int chunk_size;
    /** ?? commit time */
    private Date commitTime;
    

    /** 
     * HDFSChunk Constructor
     * 
     * @param chunkNum chunk number in the file
     * @param list replica dataNode location list
     */
    public HDFSChunk(String fileName, int chunkNum) {
        this.fileName = fileName;
        this.chunkName = fileName + "_" + Integer.toString(chunkNum);
        this.chunk_size= HDFSCommon.DFT_CHUNK_SIZE; // default chunk size = 4KB
        this.locations = new ArrayList<DataNodeInfo>();
    }
    
    /**
     * add DataNode to location list
     *
     * @param dataNode DataNodeInfo object
     */
    public void addReplicaDataNode(DataNodeInfo dataNode) {
        this.locations.add(dataNode);
        return;
    }

    /**
     * remove a replica DataNode from location list
     * @param entry DataNode to be removed from location list
     */
    public void removeReplicaDataNode(DataNodeInfo entry) {
        this.locations.remove(entry);
    }
    
    /**
     * get replica data node
     *
     * @param dataNode DataNodeInfo object
     * @return DataNodeInfo
     */
    public DataNodeInfo getReplicaDataNodeInfo(int index) {
        return this.locations.get(index);
    }
    
    
    /**
     * get replica number
     * @return number of replicas in different DataNodes
     */
    public int getReplicaNumber() {
        return this.locations.size();
    }
    
    /**
     * get chunk Name
     * @return chunk name
     */
    public String getChunkName() {
        return this.chunkName;
    }
    
    /**
     * get replica DataNode list
     * @return list of getDataNode
     */
    public ArrayList<DataNodeInfo> getReplicaDataNodes() {
        return this.locations;
    }
    
    /**
     * update Chunk size
     * @param len length of chunk size to increase
     */
    public void updateChunkSize(int len) {
        this.chunk_size += len;
    }
    
    /**
     * get current chunk size
     * @return chunk size;
     */
    public int getChunkSize() {
        return this.chunk_size;
    }
    
    /**
     * set commit time
     * @param time commit time to set
     */
    public void setCommitTime(Date time) {
        this.commitTime = time;
    }
    
    /**
     * get commit time;
     * @return commit time
     */
    public Date getCommitTime() {
        return this.commitTime;
    }
    
}
