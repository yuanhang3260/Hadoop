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

import hdfs.HDFSCommon;
import hdfs.FileIO;
import hdfs.HDFSChunk;
import hdfs.HDFSFile;
import hdfs.DFSClientInterface;

/**
 * DataNode Class
 * It provides remote services for name nodes and clients to upload, query
 * data.
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class DataNode extends UnicastRemoteObject implements DataNodeInterface {

    /** serial Version UID */
    private static final long serialVersionUID = 7965875955130649094L;
    
    /** Registry service port on DataNode */
    private Integer dataNodeRegPort;
    /** RMI service port on DataNode */
    private Integer dataNodePort;
    /** DataNOde RMI service name */
    private String dataNodeService;
    /** Storage path on DataNode */
    private String dataNodePath;
    
    /** RMI stub object. Cached once created.*/
    private NameNodeInterface nameNode;
    /** NameNode IP address */
    private String nameNodeIP;
    /** NameNode registry service port */
    private Integer nameNodeRegPort;
    /** NameNode RMI service name */
    private String nameNodeService;
    /** NameNode RMI service remote object, created when initializing DataNode */
    private Registry nameNodeRegistry;
    
    /** Current DataNode RMI service */
    private static Registry dataNodeRegistry;
    
    /** Connection cache pool of RMI services to other DataNodes.*/
    private Hashtable<String, DataNodeInterface> dataNodeList = new Hashtable<String, DataNodeInterface>();
    /** File list on this DataNode.*/
    private ConcurrentHashMap<String, HDFSFile> fileList = new ConcurrentHashMap<String, HDFSFile>();
    
    /** A flag that used to shut down this DataNode.*/
    private boolean isRunning;
    /** Slots that are assigned but not necessarily occupied.*/
    private int reservedSlot;
    
    
        
    /**
     * While isRunning is true, this data node will keep running.
     * Calling terminate() will change isRunning to false.
     * @throws RemoteException
     */
    public DataNode() throws RemoteException {
        this.isRunning = true;
    }
    
    /**
     * Used right after system started. It creates connection to name node
     * and retrieve services.
     */
    public void init() {
        // try {
        //     /* connect to NameNode */
        //     System.out.println("Connecting to name node...");
        //     this.nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeIP);
        //     this.nameNode = (NameNodeInterface) this.nameNodeRegistry.lookup(this.nameNodeService);

        //     /* register DataNode on NameNode by calling NameNode RMI "registerDataNode"*/
        //     this.nameNode.registerDataNode(InetAddress.getLocalHost().getHostAddress(), this.availableChunkSlot);
        // } catch (RemoteException | NotBoundException | UnknownHostException e) {
        //     e.printStackTrace();
        //     System.err.println("[Error]: Connecting to NameNode " + nameNodeIP + ":" + nameNodeRegPort + " failed");
        //     System.exit(-1);
        // }
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
            HDFSFile file = new HDFSFile(filename);
            file.addChunk(new HDFSChunk(filename, chunkNum));
        }

        // TODO: add replicas in other dataNodes?
        return;
    }


    /**
     * RMI call : Remove a file from this data node. 
     * @param filename String The name of the file.
     * @throws RemoteException
     */
    public void removeFile(String filename) throws RemoteException
    {
        HDFSFile file = this.fileList.get(filename);
        if (file == null) {
            System.out.println("[Error**] " + filename + "not found");
            return;
        }
   		
        ArrayList<HDFSChunk> chunklist = file.getChunkList();
        for (HDFSChunk chunk : chunklist) {
            removeChunk(chunk.getChunkName());
            file.removeChunk(chunk);
        }
        
        this.fileList.remove(filename);
        System.out.println("[LOG] " + filename + " successfully deleted");
        
        // TODO: remove replicas in other dataNodes?
        return;
    }


    /**
     * private method : Delete a specific chunk of a file from this data node. 
     * @param filename String The name of the file.
     * @param chunkNum Integer The chunk number of file to be deleted.
     * @throws RemoteException
     */
    private void removeChunk(String chunkName) throws RemoteException 
    {
        try {
            FileIO.deleteFile(this.dataNodePath + chunkName);
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("[Error**] Cannot remove " + chunkName);
            return;
        }

        System.out.println("[LOG] " + chunkName + " deleted from storage");
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
        
        ArrayList<HDFSChunk> chunklist = file.getChunkList();
        for (HDFSChunk chunk : chunklist) {
            if (chunk.getChunkName().equals(filename + "_" + chunkNum)) {
            	return true;
            }
        }
        return false;
    }

    
    /**
     * Terminate this data node.
     * @throws RemoteException
     */
    @Override
    public void terminate() {
        this.isRunning = false;
    }


    /**
     * Main method : Start up the DataNode
     */
    public static void main(String[] args) {
        // System.out.println("Starting data node server...");
        // DataNode dataNode = null;
        // try {
        //     dataNode = new DataNode();
        // } catch (RemoteException e2) {
        //     e2.printStackTrace();
        //     System.exit(-1);
        // }
        
        // //read configuration file
        // System.out.println("Loading configuration data...");
        // try {
        //     FileIO.readConf(PathConfiguration.DFSConfPath, dataNode);
        //     System.out.println("Configuration data loaded successfully...");
        // } catch (IOException e1) {
        //     e1.printStackTrace();
        //     System.out.println("Loading configuration failed...");
        //     System.exit(-1);
        // }
        
        // try {    //set up registry
        //     System.out.println("Setting up registry server...");
        //     unexportObject(dataNode, false);
        //     DataNodeInterface stub = (DataNodeInterface) exportObject(dataNode, dataNode.dataNodePort);
        //     dataNodeRegistry = LocateRegistry.createRegistry(dataNode.dataNodeRegPort);
        //     dataNodeRegistry.rebind(dataNode.dataNodeService, stub);
        // } catch (RemoteException e) {
        //     e.printStackTrace();
        //     System.err.println("System initialization failed...");
        //     System.exit(-1);
        // }
        
        // /* setup connections */
        // dataNode.init();
        
        // System.out.println("System is running...");
        // while (dataNode.isRunning) {
        //     /* doing nothing, just waiting */
        // }

        // //shutting down
        // System.out.println("System is shutting down...");
    }
}