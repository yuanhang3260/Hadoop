package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;


/**
 * DataNode RMI Interface stub
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public interface DataNodeInterface extends Remote {

	/**
     * Fetch a chunk of file.
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of file to be fetched.
     * @return byte[] The content of this file chunk.
     * @throws RemoteException
     */
    public byte[] readChunk(String filename, int chunkNum) throws RemoteException;

    /** 
     * Used to upload chunks from other data nodes or clients.
     * @param filename String The name of file to be uploaded.
     * @param chunk    byte[] The content of file chunk.
     * @param chunkNum Integer The number of chunk to be uploaded.
     * @param fromIP String The ip address where this file chunk is uploaded from.
     * @throws RemoteException
     */
    public void addChunk(String filename, byte[] chunk, int chunkNum, String fromIP, int RMIPort) throws RemoteException;
    
    /**
     * Download a file chunk from another data node.
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of this file to be download.
     * @param fromIP String The IP address to download file chunk from.
     * @throws RemoteException
     */
    public void uploadChunk(String filename, int chunkNum, String fromIP) throws RemoteException;

    /**
     * Remove a file from this data node. 
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of file to be deleted.
     * @throws RemoteException
     */
    public void removeFile(String filename, int chunkNum) throws RemoteException;
    
    /**
     * Heartbeat method to check if data node is alive.
     * @return True
     * @throws RemoteException
     */
    public boolean heartbeat() throws RemoteException;
    
    /**
     * Check if a specific chunk of file is on this data node.
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of this file.
     * @return True if exist. False if not.
     * @throws RemoteException
     */
    public boolean hasChunk(String filename, int chunkNum) throws RemoteException;
    
    /**
     * Terminate this data node.
     * @throws RemoteException
     */
    public void terminate() throws RemoteException;
}