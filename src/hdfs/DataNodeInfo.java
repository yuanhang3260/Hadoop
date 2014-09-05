package hdfs;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import hdfs.HDFSCommon;

/**
 * DataNodeInfo is a lightweight encapsulation of dataNode used
 */
public class DataNodeInfo implements Serializable {

    /** serial Version UID */
    private static final long serialVersionUID = -4553871129664598137L;
    /** registry IP address */
    public String registryIP;
    /** registry port number */
    public int registryPort;
    /** dataNode name */
    public String name;
    
    public DataNodeInfo (String ip, int port, String dataNodeName) {
        this.registryIP = ip;
        this.registryPort = port;
        this.name = dataNodeName;
    }
}
