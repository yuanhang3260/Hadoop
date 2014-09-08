package hdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import global.FileIO;
import global.Common;

import hdfs.HDFSCommon;
import hdfs.HDFSChunk;
import hdfs.HDFSFile;

/**
 * DataNode Class
 * It provides remote services for name nodes and clients to upload, query
 * data.
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class DataNode implements DataNodeInterface {

    /** serial Version UID */
    private static final long serialVersionUID = 7965875955130649094L;
    
    /** Registry service port on DataNode */
    private Integer dataNodeRegPort;
    /** DataNOde RMI service name */
    private String dataNodeService;
    
    /** RMI stub object. Cached once created.*/
    private NameNodeInterface nameNodeStub;
    /** NameNode IP address */
    private String nameNodeIP;
    /** NameNode registry service port */
    private Integer nameNodeRegPort;
    /** NameNode RMI service name */
    private String nameNodeService;
    
    /** Connection cache pool of RMI services to other DataNodes.*/
    private Hashtable<String, DataNodeInterface> dataNodeList = new Hashtable<String, DataNodeInterface>();
    /** File list on this DataNode.*/
    private ConcurrentHashMap<String, HDFSFile> fileList = new ConcurrentHashMap<String, HDFSFile>();
    
    /** A flag that used to shut down this DataNode.*/
    private boolean isRunning;
    /** Slots that are assigned but not necessarily occupied.*/
    private int reservedSlot;
    /** Storage path on DataNode */
    private String dataNodePath;
    
    
    /**
     * While isRunning is true, this data node will keep running.
     * Calling terminate() will change isRunning to false.
     * @throws RemoteException
     */
    public DataNode() throws RemoteException {
        super();
    }
    
    /** DataNode init -  */
    public void init() {
        /* read configuration file */
        System.out.println("[LOG] Loading DataNode configuration data ...");
        try {
            FileIO.readConf(Common.HDFSConfPath, this);
            System.out.println("[^_^] DataNode configured successfully");
        } 
        catch (IOException e1) {
            e1.printStackTrace();
            System.out.println("[Error**] Configuration failed");
            System.exit(-1);
        }

        /* Initialize DataNode RMI service */
        try {
            System.out.println("[LOG] Setting up DataNode RMI service on port " + dataNodeRegPort);
            DataNodeInterface dataNodeStub = (DataNodeInterface) UnicastRemoteObject.exportObject(this, 0);
            Registry dataNodeRegistry = LocateRegistry.createRegistry(dataNodeRegPort);
            /* rebind  RMi service */
            dataNodeRegistry.rebind(dataNodeService, dataNodeStub);
            System.out.println("[^_^] RMI service set up successfully");
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("[Error**] DataNode server init failed. Shutting down ...");
            System.exit(-1);
        }

        /* connect to NameNode and register it */
        try {
            /* connect to NameNode */
            System.out.println("Connecting to name node...");
            Registry nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeIP, nameNodeRegPort);
            nameNodeStub = (NameNodeInterface) nameNodeRegistry.lookup(nameNodeService);

            /* register DataNode on NameNode by calling NameNode RMI "registerDataNode"*/
            nameNodeStub.registerDataNode(InetAddress.getLocalHost().getHostAddress(), dataNodeRegPort, dataNodeService);
        }
        catch (RemoteException | NotBoundException | UnknownHostException e1) {
            e1.printStackTrace();
            System.err.println("[Error]: Connecting to NameNode " + nameNodeIP + ":" + nameNodeRegPort + " failed");
            System.exit(-1);
        }

        /* set dataNode storage path */
        dataNodePath = "./DataNode-" + dataNodeRegPort;

        /* enable running */
        isRunning = true;
    }

    
    /**
     * RMI call : Read a chunk of file.
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of file to be fetched.
     * @return byte[] The content of this file chunk.
     * @throws RemoteException
     */
    @Override
    public byte[] readChunk(String filename, int chunkNum) throws RemoteException 
    {
    	if (!hasChunk(filename, chunkNum)) {
    		return null;
    	}

        byte[] chunk;
        try {
            chunk = FileIO.readFile(this.dataNodePath + filename + "_" + chunkNum);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.println("[Error**]: Can't read chunk " + filename + "_" + chunkNum);
            throw (new RemoteException());
        }
        System.out.println("[LOG]" + "Fetched " + filename + "_" + chunkNum);
        return chunk;
    }


    /**
     * RMI call : Client or other DataNode writes chunk to this dataNode.
     * @param filename String The name of file to be uploaded.
     * @param chunk    byte[] The content of file chunk.
     * @param chunkNum Integer The number of chunk to be uploaded.
     * @param fromIP String The ip address where this file chunk is uploaded from.
     * @throws RemoteException
     */
    @Override
    public void addChunk(String filename, byte[] buf, int chunkNum, String fromIP, int clientRMIPort)
            throws RemoteException 
    {
    	if (hasChunk(filename, chunkNum)) {
    		System.err.println("[Error**] " + filename + "_" + chunkNum + " already exists");
    		return;
    	}

        try {
            /* write file on to local storage */
            FileIO.writeFile(buf, this.dataNodePath + filename + "_" + chunkNum);        
            System.err.println("[Error**] " + filename + "_" + chunkNum + " written to " + this.dataNodePath);
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.err.println("[Error**] IO exception occuring when writing file " + filename);
            throw new RemoteException("[Error] IO exception occuring when writing file " + filename);
        }
        
        // try {
        //     /* feedback acknowledge to client (really need this?) */
        //     // TODO: should put a time limit here when trying connecting to client?
        //     System.out.println("[LOG] Connecting to client " + fromIP + "...");
        //     Registry clientRegistry = LocateRegistry.getRegistry(fromIP, clientRMIPort);        
        //     DFSClientInterface client = (DFSClientInterface) clientRegistry.lookup(this.clientServiceName);
        //     client.sendChunkReceivedACK(InetAddress.getLocalHost().getHostAddress(), filename, chunkNum);
        //     System.out.println("[LOG] Client " + fromIP + " acknowledged.");
        // }
        // catch (NotBoundException | UnknownHostException e) {
        //     e.printStackTrace();
        //     System.err.println("[Error**)] Unable to connect to client");
        //     try {
        //         /* delete the chunk that has been writen to disk */
        //         FileIO.deleteFile(dataNodePath + filename + "_" + chunkNum);
        //         System.out.println("[LOG] Removed " + filename + "_" + chunkNum + "because cannot feedback to client");
        //     }
        //     catch (IOException e1) {
        //         System.out.println("[Error**] Exception occurs when removing" + filename + "_" + chunkNum);
        //     }
        //     throw new RemoteException();
        // }
        
        /* update local file list */
        if (this.fileList.containsKey(filename)) {
            HDFSFile file = this.fileList.get(filename);
            file.addChunk(new HDFSChunk(filename, chunkNum));
        }
        else {
            HDFSFile file = new HDFSFile(filename, 0);
            file.addChunk(new HDFSChunk(filename, chunkNum));
        }

        // TODO: add replicas in other dataNodes?
        return;
    }

    
    /**
     * RMI call - Delete a specific chunk of a file from this data node. 
     * @param file HDFS file.
     * @param chunkNum Integer The chunk number of file to be deleted.
     * @throws RemoteException
     */
    @Override
    public void removeChunk(String fileName, int chunkNum) throws RemoteException 
    {
        HDFSFile file = this.fileList.get(fileName);
        if (file == null) {
            System.out.println("[Error**] " + fileName + "not found");
            return;
        }

        HDFSChunk chunk = file.getChunkTable().get(chunkNum);
        if (chunk == null) {
            return;
        }

        try {
            file.removeChunk(chunkNum);
            FileIO.deleteFile(this.dataNodePath + chunk.getChunkName());
        }
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("[Error**] Cannot remove " + chunk.getChunkName());
            return;
        }

        System.out.println("[LOG] " + chunk.getChunkName() + " deleted from storage");
        return;
    }


    /**
     * RMI call : Remove a file from this data node. 
     * @param filename String The name of the file.
     * @throws RemoteException
     */
    @Override
    public void removeFile(String fileName) throws RemoteException
    {
        HDFSFile file = this.fileList.get(fileName);
        if (file == null) {
            System.out.println("[Error**] " + fileName + "not found");
            return;
        }
   		
        ConcurrentHashMap<Integer, HDFSChunk> chunkTable = file.getChunkTable();
        for (Entry<Integer, HDFSChunk> row : chunkTable.entrySet()) {
            HDFSChunk chunk = row.getValue();
            try {
                file.removeChunk(chunk.getChunkNum());
                FileIO.deleteFile(this.dataNodePath + chunk.getChunkName());
            }
            catch (IOException e) {
                e.printStackTrace();
                System.out.println("[Error**] Cannot remove " + chunk.getChunkName());
                return;
            }
        }
        
        this.fileList.remove(fileName);
        System.out.println("[LOG] " + fileName + " successfully deleted");
        
        // TODO: remove replicas in other dataNodes?
        return;
    }


    /**
     * Heartbeat send heart beat message to NameNode
     * @return file list of this DataNode
     * @throws RemoteException
     */
    @Override
    public ConcurrentHashMap<String, HDFSFile> heartBeat() throws RemoteException {
        return this.fileList;
    }


    /**
     * Check if a specific chunk of file is on this data node.
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of this file.
     * @return True if exist. False if not.
     * @throws RemoteException
     */
    @Override
    public boolean hasChunk(String filename, int chunkNum) throws RemoteException {
        HDFSFile file = this.fileList.get(filename);
        if (file == null) {
            return false;
        }
        
        ConcurrentHashMap<Integer, HDFSChunk> chunkTable = file.getChunkTable();
        if (chunkTable.containsKey(chunkNum)) {
            return true;
        }
        else {
            return false;
        }
    }

    
    /**
     * RMI call - Terminate this data node.
     * @throws RemoteException
     */
    @Override
    public void terminate() {
        this.isRunning = false;
    }


    /**
     * Main method : Start up the DataNode
     */
    public static void main(String[] args) throws RemoteException {
        System.out.println("[LOG] Starting data node server ...");
        DataNode dataNode = null;
        dataNode = new DataNode();
        
        /* setup connections */
        dataNode.init();
        System.out.println("[LOG] DataNode Initialized");

        while (dataNode.isRunning) {
            /* doing nothing, just waiting */
        }

        //shutting down
        System.out.println("[LOG] DataNode is shutting down...");
    }
}