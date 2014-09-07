package hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
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


import hdfs.FileIO;
import hdfs.HDFSClientInterface;
import util.FileStatus;
import util.FunctionalUtil;
import util.PathConfiguration;
import util.StringHandling;


/**
 * Start up Client
 * It contains a command line tool and methods to interact with HDFS.
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSClient extends UnicastRemoteObject implements DFSClientInterface {
    
    private static final long serialVersionUID = -7835407889702758301L;
    
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
    
    /** Maximum size of each chunk, read from dfs.conf*/
    private int maxChunkSize;
    /** Retry threshold for transferring chunks, read from dfs.conf*/
    private int chunkTranferRetryThreshold;
    /** Timeout threshold for DataNode's acknowledge, read from dfs.conf*/
    private int ackTimeout;
    /** NameNode RMI service remote object, created when initializing DataNode.*/
    private Registry nameNodeRegistry;
    /** RMI stub object. Cached once created.*/
    private NameNodeInterface nameNode;
    /** Connection cache pool of RMI services to DataNodes.*/
    private ConcurrentHashMap<String, DataNodeInterface> dataNodeServiceList = new ConcurrentHashMap<String, DataNodeInterface>();
    /** List of files, chunks and DataNode information to be dispatch that fetches from NameNode.*/
    private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> dispatchList = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
    
    
    public static void main(String[] args) {
        System.out.println("[LOG] Starting client server...");
        HDFSClient client = new DFSClient();
        
        System.out.println("[^_^] Welcome to use HDFS Client v0.1");
        System.out.println("[^_^] For more information, please type: \"help\"");
        while (true) {
            System.out.print("DataNode> ");
            
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String command;
            try {
                command = br.readLine();
            } catch (IOException e) {
                continue;
            }
            
            String[] cmdSplit = command.trim().split(" ");
            switch (cmdSplit[0]) {
                case "put":
                    if (cmdSplit.length == 2) {
                        client.putFile(cmdSplit[1]);
                    } 
                    else {
                        System.out.println("[Error**] number of parameters.");
                        System.out.println("Usage: put <src_file_path>");
                    }
                    break;
                case "get":
                    if (cmdSplit.length == 3) {
                        client.getFile(cmdSplit[1], cmdSplit[2]);
                    }
                    else {
                        System.out.println("[Error**] number of parameters.");
                        System.out.println("Usage: get <file_name> <target_path>");
                    }
                    break;
                case "files":
                    if (cmdSplit.length == 1) {
                        client.getFileList();
                    }
                    else {
                        System.out.println("[Error**] number of parameters.");
                        System.out.println("Usage: ls");
                    }
                    break;
                case "nodes":
                    if (cmdSplit.length == 1) {
                        client.getNodeList();
                    }
                    else {
                        System.out.println("[Error**] number of parameters.");
                        System.out.println("Usage: dfs nodes");
                    }
                    break;
                case "rm":
                    if (cmdSplit.length == 2) {
                        client.removeFile(cmdSplit[1]);
                    }
                    else {
                        System.out.println("[Error**] number of parameters.");
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
                default:
                    System.out.println("Unrecognized command. Please use \"dfs help\" to get more details.");
                    break;
            } /* end switch */
        } /* end while */
    }
    
    /**
     * This constructor is used by default.
     * @throws Exception
     */
    public HDFSClient() throws Exception {
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
            FileIO.readConf(PathConfiguration.HDFSConfPath, this);
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
            
            DFSClientInterface stub = (DFSClientInterface)exportObject(this, clientPort);
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
    private void getFileList() {
        Map<String, FileStatus> list;
        try {
            list = this.nameNode.getFileStatusTable();
        }
        catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("Cannot get file list from name node.");
            return;
        }
        System.out.println("Fetching file list from remote server...");
        System.out.println("Files on DFS are:");
        System.out.println("===================== File List ========================");
        System.out.println("File Name\tFile Status");
        for (Entry<String, FileStatus> row : list.entrySet()) {
            System.out.println(row.getKey() + "    " + row.getValue());
        }
        System.out.println("======================= End ============================");
        return;
    }
    

    /**
     * Get the node list from NameNode
     */
    private void getNodeList() {
        ConcurrentHashMap<String, Integer> list;
        try {
            list = this.nameNode.getDataNodeAvailableSlotList();
        } catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("Cannot get available slot list from name node.");
            return;
        }
        System.out.println("Fetching data node list from remote server...");
        System.out.println("Data nodes in DFS are:");
        System.out.println("===================== Node List ========================");
        System.out.println("Data Node\tAvailable Slots");
        for (Entry<String, Integer> row : list.entrySet()) {
            System.out.println(row.getKey() + "    " + row.getValue());
        }
        System.out.println("======================= End ============================");
        return;
    }
    
    
    /**
     * Put a file from local to DFS.
     * @param filePath String The path of input file.
     */
    public void putFile(String filePath) {
        String filename = StringHandling.getFileNameFromPath(filePath);
        ArrayList<Long> split = calculateFileSplit(filePath);
        
        //get dispatching list from name node
        try {
            System.out.println("Requesting distribution list from name node: " + nameNodeIP + "...");
            Hashtable<Integer, HashSet<String>> tempList = this.nameNode.generateChunkDistributionList(filename, split.size() - 1).get(filename);
            dispatchList.put(filename, tempList);
            System.out.println("Dispatch list received.");
        } catch (RemoteException e) {
            e.printStackTrace();
            System.err.println("Exception occurs when fetching distribution table...");
            return;
        }
        
        System.out.println("Dispatching file...");
        if (dispatchList != null && dispatchList.size() > 0) {
            try {
                dispatchChunks(filePath, split);
                dispatchList = null;
                System.out.println(filePath + " has been sucessfully uploaded to DFS.");
            } catch (RemoteException | FileNotFoundException e) {
                System.err.println("Dispatching file failed.");
                return;
            }
        } else {
            System.out.println("File dispatch error. Please try again.");
            return;
        }
    }
    
    
    /**
     * Get a file from DFS.
     * @param file String The path of input file on DFS.
     */
    private void getFile(String filename, String outPath) {
        if (!outPath.endsWith("/")) {
            outPath += "/";
        }
        
        ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistribution;
        try {
            fileDistribution = this.nameNode.getFileDistributionTable();
        } catch (RemoteException e2) {
            e2.printStackTrace();
            System.out.println("Exception occurs when fetching file.");
            return;
        }
        
        if (fileDistribution.containsKey(filename)) {
            int chunkCount = fileDistribution.get(filename).size();
            for (int chunkNum = 0; chunkNum < chunkCount; chunkNum++) {
                System.out.println("Fetching chunk" + chunkNum + " of file\"" + filename + "\"...");
                HashSet<String> nodeList = fileDistribution.get(filename).get(chunkNum);
                byte[] chunk = null;
                
                for (String dataNodeIP : nodeList) {
                    //Setup remote services of data nodes
                    try {
                        DataNodeInterface dataNode = getDataNodeService(dataNodeIP);
                        chunk = dataNode.getFile(filename, chunkNum);
                        FileIO.appendBytesToFile(outPath + filename, chunk);
                        break;
                    } catch (IOException e) {    //if writing file chunk to storage failed, remove it
                        System.err.println("Exception occurs when downloading file...");
                        try {
                            FileIO.deleteFile(outPath + filename);
                        } catch (IOException e1) {
                            System.err.println("Cannot delete " + filename + "_" + chunkNum + " from " + dataNodeIP);
                        }
                    } catch (NotBoundException e) {
                        continue;
                    }
                }
            }
        }
    }
    
    
    /**
     * Delete a file on DFS.
     * @param file String The path of file to be deleted.
     */
    private void removeFile(String filename) {
        ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistribution;
        try {
            fileDistribution = this.nameNode.getFileDistributionTable();
        } catch (RemoteException e1) {
            e1.printStackTrace();
            System.out.println("Cannot remove file...");
            return;
        }
        if (fileDistribution.containsKey(filename)) {
            for (Entry<Integer, HashSet<String>> chunkTuple : fileDistribution.get(filename).entrySet()) {
                int chunkNum = chunkTuple.getKey();
                //byte[] chunk = null;
                for (String dataNodeIP : chunkTuple.getValue()) {
                    try {
                        //Setup remote services of data nodes
                        DataNodeInterface dataNode = getDataNodeService(dataNodeIP);
                        dataNode.removeChunk(filename, chunkNum);
                        nameNode.removeChunkFromFileDistributionTable(filename, chunkNum, dataNodeIP);
                    } catch (Exception e) {
                        System.err.println("Exception occurs when removing files. Please try again.");
                        return;
                    }
                }
            }
        }
    }
    
    
    /**
     * Generate a list of split offset of the input file.
     * @param file The path of input file.
     * @return A list of offset of input file.
     */
    private ArrayList<Long> calculateFileSplit(String filePath) {
        try {
            return FileIO.calculateFileSplit(filePath, this.maxChunkSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    
    /**
     * Dispatch file chunks to data nodes per name node's instruction. The Client is responsible
     * for guaranteeing the succeed of transfer. Whenever a chunk is successfully transfered, 
     * the client should receive an acknowledge from the data node. In the case when failures happened,
     * the client will first try to re-send the file chunk. After retried three times, the client will send back 
     * the rest of the list to name node for re-allocation and try to dispatch again.
     * @param dispatchList A list provided by NameNode towards dispatching file chunks.
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    private void dispatchChunks(String filePath, ArrayList<Long> splitStartPointOffset) throws RemoteException, FileNotFoundException {
        String filename = StringHandling.getFileNameFromPath(filePath);
        RandomAccessFile file;
        byte[] chunk;
        
        try {
            file = new RandomAccessFile(filePath, "r");
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            System.err.println("File not found.");
            throw e1;
        }
        
        //guaranteed to dispatch all the chunks. if failed, get new dispatch list and keep dispatching
        ConcurrentHashMap<String,Hashtable<Integer,HashSet<String>>> dispatchListDeepCopy = FunctionalUtil.deepCopy(dispatchList);
        while (dispatchListDeepCopy.get(filename).size() > 0) {
            for (Entry<Integer, HashSet<String>> chunkTuple : dispatchListDeepCopy.get(filename).entrySet()) {
                int chunkNum = chunkTuple.getKey();
                int chunkSize = 0;
                
                try {            
                    //obtain the chuck to be sent
                    chunkSize = (int) (splitStartPointOffset.get(chunkNum + 1) - splitStartPointOffset.get(chunkNum));
                    long startPos = splitStartPointOffset.get(chunkNum);
                    chunk = FileIO.readChunk(file, startPos, chunkSize);
                } catch (IOException e1) {
                    e1.printStackTrace();
                    System.err.println("IO exception occurs when fetching chunk" + chunkNum);
                    continue;
                }

                for(String dataNodeIP : chunkTuple.getValue()){
                    int retryThreshold = this.chunkTranferRetryThreshold;    //limit the times of retry
                    boolean success = false;

                    DataNodeInterface node;
                    try {        //Setup remote services of data nodes
                        node = getDataNodeService(dataNodeIP);
                    } catch (RemoteException | NotBoundException e1) {
                        System.err.println("Cannot connect to " + dataNodeIP + ".");
                        e1.printStackTrace();
                        continue;
                    }    
                    
                    
                    //Retry if failed as long as retry threshold not met.
                    while (!success && retryThreshold > 0) {        
                        try {
                            //start transferring chunk. 
                            System.out.println("Dispatching chunk" + chunkNum + " of file \"" + filename + "\" to " + dataNodeIP + "...");
                            node.uploadChunk(filename, chunk, chunkNum, InetAddress.getLocalHost().getHostAddress(), clientRegPort);
                            System.out.println("Chunk" + chunkNum + " of file \"" + filename + "\" has been uploaded to " + dataNodeIP + ".");
                            success = true;
                            
                            
                            //waiting for dataNode acknowledge
                            long timeoutExpiredMs = System.currentTimeMillis() + (ackTimeout * 1000);    
                            System.out.println("Waitting for " + dataNodeIP + "'s acknowledge...");
                            while (System.currentTimeMillis() < timeoutExpiredMs) {
                                //check if data node acknowledged received
                                if (this.dispatchList.containsKey(filename)
                                        && this.dispatchList.get(filename).containsKey(chunkNum)
                                        && this.dispatchList.get(filename).get(chunkNum).contains(dataNodeIP)) {
                                    if (System.currentTimeMillis() < timeoutExpiredMs) {
                                        Thread.sleep(2 * 1000);
                                    } else {
                                        retryThreshold--;
                                        System.out.println("Upload timeout. Retrying for " +
                                                (this.chunkTranferRetryThreshold - retryThreshold) + " times...");
                                        continue;
                                    }
                                } else {
                                    success = true;
                                    break;
                                }
                            }
                        } catch (RemoteException | UnknownHostException e) {
                            retryThreshold--;
                            e.printStackTrace();
                            System.err.println("Exception occurs when uploading file. Retrying for " + 
                                    (this.chunkTranferRetryThreshold - retryThreshold) + " times...");
                        } catch (InterruptedException e) {
                            retryThreshold--;
                            System.err.println("Timer error. Retrying for " + 
                                    (this.chunkTranferRetryThreshold - retryThreshold) + " times...");
                        }
                    }
                    
                    if (retryThreshold <= 0) {        //after retries, print out error message
                        System.err.print("Upload chunk" + chunkNum + " to " + dataNodeIP + " failed.");
                        throw new RemoteException();
                    }
                }
            }
            
            if (!dispatchList.containsKey(filename) || dispatchList.get(filename).size() == 0) {
                //dispatch finished
                System.out.println("Dispatch finished.");
                break;
            } else {
                //Send back failure list to name node for new dispatching list.
                try {
                    System.out.println("Re-generating new dispatch list...");
                    ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
                    failureList.put(filename, dispatchList.get(filename));
                    dispatchList.put(filename, nameNode.generateChunkDistributionList(failureList).get(filename));
                    System.out.println("New distribution list is received.");
                } catch (RemoteException e) {
                    System.err.println("System run out of storage space!");
                    throw e;
                }
            }
        }
        
        try {    //acknowledge name node
            nameNode.fileDistributionConfirm(filename);        
            System.out.println("Acknowledged name node " + nameNodeIP + "...");
        } catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("Cannot acknowledge name node.");
        }
        return;
    }
    
    @Override
    public void sendChunkReceivedACK(String fromIP, String filename, int chunkNum) {
        if (this.dispatchList != null) {
            if (this.dispatchList.containsKey(filename)) {
                if (this.dispatchList.get(filename).containsKey(chunkNum)) {
                    if (this.dispatchList.get(filename).get(chunkNum).contains(fromIP)) {
                        if (this.dispatchList.get(filename).get(chunkNum).size() == 1) {
                            this.dispatchList.get(filename).remove(chunkNum);
                        } else {
                            this.dispatchList.get(filename).get(chunkNum).remove(fromIP);
                        }
                        return;
                    }
                }
            }
        }
        System.err.println("Dispatch record not found.");
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
                System.err.println("Cannot connect to " + dataNodeIP + "...");
                throw e;
            }
        }
        return this.dataNodeServiceList.get(dataNodeIP);
    }
}