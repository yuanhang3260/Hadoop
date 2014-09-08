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
     * List all files in HDFS
     * @throws RemoteException
     */
    public HashMap<String, HDFSFile> listFiles() throws RemoteException;

    /**
     * List all active DataNodes currently registered in NameNode
     * @throws RemoteException
     */
    public HashMap<String, DataNodeInfo> listDataNodes() throws RemoteException;

    /**
     * get file from HDFS
     * @throws RemoteException
     */
    public HDFSFile getFile(String fileName) throws RemoteException;

    /**
     * create file on HDFS
     * @throws RemoteException
     */
    public HDFSFile createFile(String fileName, int size) throws RemoteException;

    /**
     * remove file on HDFS
     * @throws RemoteException
     */
    public HDFSFile removeFile(String fileName) throws RemoteException;

}