package hdfs.io;

import global.Hdfs;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class HDFSChunk implements Serializable {

	private static final long serialVersionUID = -253895999225595586L;
	private String chunkName;
//	private String fileName;
	private List<DataNodeEntry> locations;
//	private int replicaFactor;
	private int chunk_size;
	private Date commitTime;
	

	
	public HDFSChunk(String chunkName, List<DataNodeEntry> list) {
		locations = list;
		this.chunkName = chunkName;
		this.chunk_size= Hdfs.Core.CHUNK_SIZE;
	}
	
	public void addDataNode(String ip, int port, String name) {
		DataNodeEntry newDataNode = new DataNodeEntry(ip, port, name);
		this.locations.add(newDataNode);
		return;
	}
	
	public DataNodeEntry getDataNode(int index) {
		return this.locations.get(index);
	}
	
	
	public int getReplicaNumber() {
		return this.locations.size();
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
	
	public List<DataNodeEntry> getAllLocations() {
		return this.locations;
	}
	
	public void updateChunkSize(int len) {
		this.chunk_size += len;
	}
	
	public int getChunkSize() {
		return this.chunk_size;
	}
	
	public void removeDataNodeEntry(DataNodeEntry entry) {
		
		this.locations.remove(entry);
		
	}
	
	public void setCommitTime(Date time) {
		this.commitTime = time;
	}
	
	public Date getCommitTime() {
		return this.commitTime;
	}
	
}
