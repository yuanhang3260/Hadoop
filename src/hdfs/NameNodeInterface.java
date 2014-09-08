package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import hdfs.HDFSFile;
import hdfs.DataNode;
import hdfs.DataNodeInfo;


/**
 * NameNode RMI interface
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public interface NameNodeInterface extends Remote {
    
    /**
     * Register DataNode on NameNode
     * @param dataNodeIP dataNode IP address
     * @param dataNodeRegPort dataNode RMI service registry port
     * @param dataNodeService dataNode service name
     * @throws RemoteException
     */
    public void registerDataNode(String dataNodeIP, int dataNodeRegPort, String dataNodeService) 
                                 throws RemoteException;

    /**
     * List all files in HDFS
     * @throws RemoteException
     */
    public ConcurrentHashMap<String, HDFSFileMeta> getFileTable() throws RemoteException;

    /**
     * List all active DataNodes currently registered in NameNode
     * @throws RemoteException
     */
    public ConcurrentHashMap<String, DataNodeInfo> getDataNodeTable() throws RemoteException;

    /**
     * get file from HDFS
     * @throws RemoteException
     */
    public HDFSFile getFile(String fileName) throws RemoteException;

    /**
     * create file on HDFS
     * @throws RemoteException
     */
    public HDFSFile createFile(String fileName, long size) throws RemoteException;

    /**
     * remove file on HDFS
     * @throws RemoteException
     */
    public HDFSFile removeFile(String fileName) throws RemoteException;

    /** terminate the nameNode */
    public void terminate();

}