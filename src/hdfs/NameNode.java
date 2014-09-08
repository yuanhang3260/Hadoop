package hdfs;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import global.FileIO;
import global.Common;

import hdfs.HDFSFile;
import hdfs.HDFSChunk;
import hdfs.DataNode;
import hdfs.DataNodeInterface;
import hdfs.DataNodeInfo;
import hdfs.HDFSClientInterface;


/**
 * Name Node
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class NameNode extends UnicastRemoteObject implements NameNodeInterface {
    /** serial Version UID */
    private static final long serialVersionUID = 455874693232909953L;
    /** running status */
    private static boolean isRunning;

    /** NameNode registry service port, read from dfs.conf*/
    private static Integer nameNodeRegPort;
    /** NameNode RMI service name, read from dfs.conf*/
    private static String nameNodeService;

    /** DataNode Table */
    ConcurrentHashMap<String, DataNodeInfo> dataNodeTable = new ConcurrentHashMap<String, DataNodeInfo>();
    /** file Table */
    ConcurrentHashMap<String, HDFSFile> fileTable = new ConcurrentHashMap<String, HDFSFile>();
    /** file meta Table */
    ConcurrentHashMap<String, HDFSFileMeta> fileMetaTable = new ConcurrentHashMap<String, HDFSFileMeta>();

    /** Constructor */
    public NameNode() throws RemoteException {
        super();
    }

    /** Init NameNode */
    public void init() {
        /* Load configuration */
        System.out.println("[LOG] Loading NameNode configuration data ...");
        try {
            FileIO.readConf(Common.HDFSConfPath, this);
            System.out.println("[^_^] NameNode configured successfully");
        }
        catch (IOException e1) {
            e1.printStackTrace();
            System.err.println("[Error**] Configuration failed");
            System.exit(-1);
        }

        /* Initialize NameNode RMI service */
        try {
            System.out.println("[LOG] Setting up NameNode RMI service on port" + nameNodeRegPort);
            NameNodeInterface nameNodeStub = (NameNodeInterface) UnicastRemoteObject.exportObject(this, 0);
            Registry nameNodeRegistry = LocateRegistry.createRegistry(nameNodeRegPort);
            /* rebind  RMi service */
            nameNodeRegistry.rebind(nameNodeService, nameNodeStub);
            System.out.println("[^_^] RMI service successfully set up");
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("[Error**] NameNode server init failed. Shutting down ...");
            System.exit(-1);
        }

        /* enable running */
        isRunning = true;
    }


    /** 
     * RMI call - register DataNode on NameNode
     * @param dataNoeIP DataNode IP address
     */
    @Override
    public void registerDataNode(String dataNodeIP, int dataNodeRegPort, String dataNodeService) 
                                 throws RemoteException
    {
        this.dataNodeTable.put(dataNodeIP, 
                               new DataNodeInfo(dataNodeIP, dataNodeRegPort,dataNodeService)
                               );
        System.out.println("[LOG] "+ dataNodeIP + ":" + dataNodeRegPort + " added to DataNode list");
    }


    /**
     * RMI call - List all files in HDFS
     * @throws RemoteException
     */
    @Override
    public ConcurrentHashMap<String, HDFSFileMeta> getFileTable() throws RemoteException {
        return this.fileMetaTable;
    }


    /**
     * RMI call - List all active DataNodes currently registered in NameNode
     * @throws RemoteException
     */
    @Override
    public ConcurrentHashMap<String, DataNodeInfo> getDataNodeTable() throws RemoteException {
        return this.dataNodeTable;
    }


    /**
     * RMI call - get file from HDFS
     * @throws RemoteException
     * @param fielName file name
     * @return HDFSFile or null if fileName does not exist
     */
    @Override
    public HDFSFile getFile(String fileName) throws RemoteException {
        return fileTable.get(fileName);
    }


    /**
     * RMI call - create file on HDFS
     * @throws RemoteException
     */
    @Override
    public HDFSFile createFile(String fileName, long size) throws RemoteException {
        //TODO - need carefull workflow design
        return null;
    }


    /**
     * RMI call - remove file on HDFS
     * @throws RemoteException
     */
    @Override
    public HDFSFile removeFile(String fileName) throws RemoteException {
        //TODO - need carefull workflow design
        if (fileTable.containsKey(fileName)) {
            HDFSFile file = fileTable.get(fileName);
            fileTable.remove(fileName);
            fileMetaTable.remove(fileName);
            return file;
        }
        return null;
    }


    /** RMI call - terminate the NameNode */
    @Override
    public void terminate() {
        isRunning = false;
    }


    /** start up NameNode */
    public static void main(String[] args) throws RemoteException {
        System.out.println("[LOG] Starting name node server...");
        NameNode nameNode = new NameNode();
        
        /* init */
        nameNode.init();
        System.out.println("[LOG] NameNode Initialized");

        while (isRunning) {
            /* do nothing */
        }
        System.out.println("NameNode is shuting down...");
    }
}