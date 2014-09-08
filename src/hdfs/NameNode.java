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

    /** Constructor */
    public NameNode() throws RemoteException {
        super();
    }

    /** Init NameNode */
    public init() {
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
    public void registerDataNode(String dataNodeIP) throws RemoteException {
        this.dataNodeAvailableSlotList.put(dataNodeIP, availableSlot);
        this.dataNodeStatusList.put(dataNodeIP);
        System.out.println("[LOG] "+ dataNodeIP + " added to DataNode list");
    }

    /** stop the NameNode */
    @Override
    public void terminate() {
        isRunning = false;
    }


    /** start up NameNode */
    public static void main(String[] args) throws RemoteException {
        System.out.println("Starting name node server...");
        NameNode nameNode = new NameNode();

        /* init */
        nameNode.init();

        System.out.println("System is running...");
        while (isRunning) {

        }
        System.out.println("System is shuting down...");
    }
}