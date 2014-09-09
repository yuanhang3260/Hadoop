package hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


import hdfs.HDFSFileMeta;
import hdfs.DataNode;
import hdfs.DataNodeInfo;
import hdfs.HDFSClientInterface;

import global.Common;
import global.FileIO;

/**
 * Start up Client
 * It contains a command line tool and methods to interact with HDFS.
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSClient extends UnicastRemoteObject implements HDFSClientInterface{
    
    private static final long serialVersionUID = -7835407889702758301L;
    
    /** DFSClient IP address */
    private String clientIP;
    /** DFSClient registry service port, read from dfs.conf*/
    private int clientRegPort;
    /** DFSClient RMI service port, read from dfs.conf*/
    private int clientPort;
    /** DFSClient RMI service name, read from dfs.conf*/
    private String clientServiceName;
    
    /** dataNode IP address, read from dfs.conf*/
    private String dataNodeIP;
    /** dataNode RMI port, read from dfs.conf*/
    private Integer dataNodeRegPort;
    /** DataNOde RMI service name, read from dfs.conf*/
    private String dataNodeService;
    
    /** NameNode IP address, read from dfs.conf*/
    private String nameNodeIP;
    /** NameNode registry service port, read from dfs.conf*/
    private Integer nameNodeRegPort;
    /** NameNode RMI service name, read from dfs.conf*/
    private String nameNodeService;
    /** NameNode RMI service remote object, created when initializing DataNode.*/
    private Registry nameNodeRegistry;
    /** RMI stub object. Cached once created.*/
    private NameNodeInterface nameNode;

    /** Maximum size of each chunk, read from dfs.conf*/
    private int maxChunkSize;
    /** Retry threshold for transferring chunks, read from dfs.conf*/
    private int chunkTranferRetryThreshold;
    /** Timeout threshold for DataNode's acknowledge, read from dfs.conf*/
    private int ackTimeout;
    
    /** Connection cache pool of RMI services to DataNodes.*/
    private ConcurrentHashMap<String, DataNodeInterface> dataNodeServiceList = new ConcurrentHashMap<String, DataNodeInterface>();
    /** List of files, chunks and DataNode information to be dispatch that fetches from NameNode.*/
    private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> dispatchList = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
    
    
    public static void main(String[] args) throws Exception, IOException {
        System.out.println("[LOG] Starting client server...");
        HDFSClient client = null;
        try {
            client = new HDFSClient();
        }
        catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("[^_^] Welcome to use HDFS Client v0.1");
        System.out.println("[^_^] For more information, please type: \"help\"");
        boolean exit = false;
        while (!exit) {
            System.out.print("HDFSClient> ");
            
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String command;
            try {
                command = br.readLine();
            }
            catch (IOException e) {
                continue;
            }
            
            String[] cmdSplit = command.trim().split(" ");
            switch (cmdSplit[0]) {
                case "put":
                    if (cmdSplit.length == 2) {
                        client.putFile(cmdSplit[1]);
                    } 
                    else {
                        System.out.println("[Error**] Invalid number of parameters.");
                        System.out.println("Usage: put <src_file_path>");
                    }
                    break;
                case "get":
                    if (cmdSplit.length == 2) {
                        client.getFile(cmdSplit[1]);
                    }
                    else {
                        System.out.println("[Error**] Invalid number of parameters.");
                        System.out.println("Usage: get <file_name> <target_path>");
                    }
                    break;
                case "files":
                    if (cmdSplit.length == 1) {
                        client.getFileList();
                    }
                    else {
                        System.out.println("[Error**] Invalid number of parameters.");
                        System.out.println("Usage: ls");
                    }
                    break;
                case "nodes":
                    if (cmdSplit.length == 1) {
                        client.getNodeList();
                    }
                    else {
                        System.out.println("[Error**] Invalid number of parameters.");
                        System.out.println("Usage: dfs nodes");
                    }
                    break;
                case "rm":
                    if (cmdSplit.length == 2) {
                        client.removeFile(cmdSplit[1]);
                    }
                    else {
                        System.out.println("[Error**] Invalid number of parameters.");
                        System.out.println("Usage: rm <file_name>");
                    }
                    break;
                case "help":
                    System.out.println("\"put\": put a file from local on to DFS.");
                    System.out.println("Usage: dfs put <file_path>");
                    
                    System.out.println("\"get\": get a file from DFS to local.");
                    System.out.println("Usage: dfs get <file_name> <target_path>");
                    
                    System.out.println("\"files\": list file list on DFS.");
                    System.out.println("Usage: dfs files");
                    
                    System.out.println("\"nodes\": list data node list on DFS.");
                    System.out.println("Usage: dfs nodes");
                    
                    System.out.println("\"rm\": remove a file on DFS.");
                    System.out.println("Usage: dfs rm <file_name>");
                    break;
                case "exit":
                    //exit = true;
                    break;
                default:
                    System.out.println("Unknown command. Please use \"dfs help\" to get more details.");
                    break;
            } /* end switch */
        } /* end while */
    }
    
    /**
     * This constructor is used by default.
     * @throws Exception
     */
    public HDFSClient() throws IOException, Exception {
        loadConf();
        init();
    }
    
    /**
     * Load configuration data from util.PathConfiguration.
     * @throws IOException
     */
    public void loadConf() throws IOException {
        System.out.println("[LOG] Loading configuration data ...");
        try {
            FileIO.readConf(Common.HDFSConfPath, this);
            System.out.println("[^_^] Configured successfully");
        }
        catch (IOException e) {
            throw e;
        }
    }
    
    /**
     * Setup client RMI service and connections to name node.
     * @throws Exception
     */
    public void init() throws Exception {
        try {
            /* connect to Name Node server */
            System.out.println("[LOG] Connecting to Name Node server ...");
            this.nameNodeRegistry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
            this.nameNode = (NameNodeInterface)nameNodeRegistry.lookup(nameNodeService);
            System.out.println("[^_^] Connected successfully");
        }
        catch (NotBoundException | RemoteException e) {
            throw e;
        }
        
        /* set up client registry server */
        try {
            System.out.println("[LOG] Initializing client registry server ...");
            unexportObject(this, false);
            
            HDFSClientInterface stub = (HDFSClientInterface) UnicastRemoteObject.exportObject(this, clientPort);
            Registry clientRegistry = LocateRegistry.createRegistry(clientRegPort);
            clientRegistry.rebind(clientServiceName, stub);
            System.out.println("[^_^] Registry server set up on port: " + clientRegPort);
        }
        catch (Exception e) {
            throw e;
        }
    }
    

    /**
     * Get the file list in HDFS through NameNode RMI call.
     */
    public void getFileList() throws RemoteException {
        ConcurrentHashMap<String, HDFSFileMeta> list = null;
        try {
            list = this.nameNode.getFileTable();
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("[Error**] Can't get file list from Name Node " + nameNodeIP);
            return;
        }
        System.out.println("===================== File List ========================");
        System.out.printf("%-15s %-15s %-15s\n", "Name", "Size", "M_Time");
        for (Entry<String, HDFSFileMeta> row : list.entrySet()) {
            HDFSFileMeta fileInfo = row.getValue();
            System.out.printf("%-15s %-15s %-15s", fileInfo.getName(), fileInfo.getSize(), fileInfo.getModTime());
        }
        System.out.println("======================= End ============================");
        return;
    }
    

    /**
     * Get the node list from NameNode
     */
    public void getNodeList() throws RemoteException {
        ConcurrentHashMap<String, DataNodeInfo> list = null;
        try {
            list = this.nameNode.getDataNodeTable();
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.err.println("[Error**] Can't get DataNode list");
            return;
        }
        System.out.println("===================== Node List ========================");
        for (Entry<String, DataNodeInfo> row : list.entrySet()) {
            DataNodeInfo dnInfo = row.getValue();
            System.out.printf("%7s  %s:%s\n", dnInfo.name, dnInfo.registryIP, dnInfo.registryPort);
        }
        System.out.println("======================= End ============================");
        return;
    }
    
    
    /**
     * Download a file from HDFS.
     * @param fileName file Name on HDFS
     */
    public void getFile(String fileName) throws RemoteException {

        HDFSFile file = null;
        try {
            file = this.nameNode.getFile(fileName);
        }
        catch (RemoteException e2) {
            e2.printStackTrace();
            System.out.println("[Error**] Exception occurs when fetching file " + fileName);
            return;
        }
        
        ConcurrentHashMap<Integer, HDFSChunk> chunkTable = file.getChunkTable();
        for (int i = 0; i < chunkTable.size(); i++) {

            HDFSChunk chunk = chunkTable.get(i);
            System.out.println("[LOG] Fetching chunk " + chunk.getChunkName() + " ...");
            byte[] buf = new byte[chunk.getChunkSize()];

            /** Try fetching data from at most 3 replicas in different dataNodes */
            for (DataNodeInfo dataNodeInfo : chunk.getReplicaDataNodes()) {
                try {
                    /* get dataNode RMI stub */
                    DataNodeInterface dataNode = getDataNodeService(dataNodeInfo.registryIP);
                    /* DataNode RMI call : readChunk(String fileName, int chunkNum) */
                    buf = dataNode.readChunk(fileName, i);
                    /* append data to local output file */
                    FileIO.appendFile(Common.LocalFSPath + fileName, buf);
                    /* go to next chunk */
                    break;
                }
                catch (IOException e) {
                    System.err.println("[Error**] Exception occurs when downloading file...");
                    try {
                        FileIO.deleteFile(Common.LocalFSPath + fileName);
                        return;
                    }
                    catch (IOException e1) {
                        System.err.println("[Error**] Cannot delete " + fileName);
                        return;
                    }
                }
                catch (NotBoundException e) {
                    continue;
                }
            }
        }
    }
    

    /**
     * Upload a file from local to HDFS.
     * @param fileName path of local input file.
     */
    public void putFile(String fileName) throws RemoteException {
        /* import local file */
        File file = new File(Common.LocalFSPath + fileName);

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
        }
        catch (FileNotFoundException e) {
            System.err.println("[Error**] File " + fileName + " does not exist!");
            return;
        }
        
        /* RMI call - create hdfs File on NameNode and get chunk distribution 
         * in HDFSFile Object returned bt NameNoe */
        HDFSFile hdfsFile = null;
        try {
            hdfsFile = this.nameNode.createFile(fileName, file.length());
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("[Error**] Exception occurs when fetching file " + fileName);
            return;
        }

        ConcurrentHashMap<Integer, HDFSChunk> chunkTable = hdfsFile.getChunkTable();

        /* begin pushing data to DataNodes based on meta data got from NameNode */
        for (int i = 0; i < chunkTable.size(); i++) {
            HDFSChunk chunk = chunkTable.get(i);
            byte[] content = new byte[(int)chunk.getChunkSize()];

            try {
                fis.read(content);
            }
            catch (IOException e1) {
                e1.printStackTrace();
                return;
            }
            /* write 3 replicas */
            for (DataNodeInfo dataNodeInfo : chunk.getReplicaDataNodes()) {
                try {
                    /* get dataNode RMI stub */
                    DataNodeInterface dataNode = getDataNodeService(dataNodeInfo.registryIP);
                    /* DataNode RMI call : addChunk */
                    dataNode.addChunk(fileName, content, chunk.getChunkNum(), clientIP, clientRegPort);
                }
                catch (RemoteException | NotBoundException e2) {
                    //TODO - how to handle?
                    e2.printStackTrace();
                    continue;
                }
            }
        }
        return;
    }

    
    /**
     * Delete a file on DFS.
     * @param fileName String The path of file to be deleted.
     */
    public void removeFile(String fileName) {
        HDFSFile hdfsFile = null;
        try {
            hdfsFile = this.nameNode.removeFile(fileName);
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("[Error**] Exception occurs when fetching file " + fileName);
            return;
        }

        /* remove chunks from dataNode */
        ConcurrentHashMap<Integer, HDFSChunk> chunkTable = hdfsFile.getChunkTable();
        for (Entry<Integer, HDFSChunk> row : chunkTable.entrySet()) {
            
            HDFSChunk chunk = row.getValue();
            /* remove all three replicas */
            for (DataNodeInfo dataNodeInfo : chunk.getReplicaDataNodes()) {
                try {
                    /* get dataNode RMI stub */
                    DataNodeInterface dataNode = getDataNodeService(dataNodeInfo.registryIP);
                    /* DataNode RMI call : addChunk */
                    dataNode.removeChunk(fileName, chunk.getChunkNum());
                }
                catch (RemoteException | NotBoundException e2) {
                    //TODO - how to handle?
                    e2.printStackTrace();
                    continue;
                }
            }
        }
        return;
    }
    
    
    /**
     * Cache all the connections to data node.
     * @param dataNodeIP String The IP address of data node.
     * @return DataNodeInterface The remote object reference of data node.
     * @throws RemoteException
     * @throws NotBoundException
     */
    private DataNodeInterface getDataNodeService(String dataNodeIP) throws RemoteException, NotBoundException {
        if (!this.dataNodeServiceList.containsKey(dataNodeIP)) {
            try {
                Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNodeIP, this.dataNodeRegPort);
                DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(this.dataNodeService);
                this.dataNodeServiceList.put(dataNodeIP, dataNode);
            } catch (RemoteException | NotBoundException e) {
                System.err.println("[Error**] Can't connect to DataNode" + dataNodeIP);
                throw e;
            }
        }
        return this.dataNodeServiceList.get(dataNodeIP);
    }
}
