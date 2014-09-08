package global;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * Configuration class for all configurations read from hdfs.xml
 */
public class HDFSConf {
    
    public static String NAME_NODE_SERVICE_NAME = "NameNode";
    
    public static String DATA_NODE_SERVICE_NAME = "DataNode";

    public static int CHUNK_SIZE = 4096;
    
    public static long HEART_BEAT_FREQ = 1;
    
    public static long SYSTEM_CHECK_PERIOD = 20;

    public static String NAME_NODE_IP = "127.0.0.1";
    
    public static int NAME_NODE_REGISTRY_PORT = 1099;

    public static String DATA_NODE_IP = "127.0.0.1";
    
    public static int DATA_NODE_REGISTRY_PORT = 9090;

        
    // public static class ConfFormatException extends Exception {
    //     private static final long serialVersionUID = 8494918500253529934L;

    //     public ConfFormatException(String message) {
    //         super(message);
    //     }
    // }

    // public static void LoadHDFSConf() throws ConfFormatException {
        
    //     InetAddressValidator ipValidator = new InetAddressValidator();
        
    //     File fXmlFile = new File("./conf/hdfs.xml");
    //     DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    //     DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    //     Document doc = dBuilder.parse(fXmlFile);
    //     doc.getDocumentElement().normalize();
    //     NodeList nList = doc.getElementsByTagName("core");
        
    //     for (int i = 0; i < nList.getLength(); i++) {
            
    //         Node nNode = nList.item(i);
            
    //         if (nNode.getNodeType() == Node.ELEMENT_NODE) {
    //             Element eElement = (Element) nNode;
                
    //             /* Parse chunk size */
    //             Hdfs.Core.CHUNK_SIZE =
    //                     Integer.parseInt(eElement.getElementsByTagName("chunk-size")
    //                             .item(0).getTextContent().trim());
                
    //             String unitType = 
    //                     ((Element)eElement.getElementsByTagName("chunk-size").item(0)).getAttribute("unit");
    //             int unit = parseUnit(unitType);
    //             Hdfs.Core.CHUNK_SIZE *= unit;
                
    //             /* Parse hear beat freq */
    //             Hdfs.Core.HEART_BEAT_FREQ =
    //                     Integer.parseInt(eElement.getElementsByTagName("heartbeat-freq")
    //                             .item(0).getTextContent().trim());
                
    //             unitType = 
    //                     ((Element)eElement.getElementsByTagName("heartbeat-freq").item(0)).getAttribute("unit");
    //             unit = parseUnit(unitType);
    //             Hdfs.Core.HEART_BEAT_FREQ *= unit;
                
    //             if (Hdfs.Core.HEART_BEAT_FREQ < 1) {
    //                 throw new ConfFormatException ("heart beat frequency cannot be non-positive");
    //             }
                
    //             Hdfs.Core.INCONSISTENCY_LATENCY = Hdfs.Core.HEART_BEAT_FREQ * 6;
                
    //             /* Parse system check freq */
    //             Hdfs.Core.SYSTEM_CHECK_PERIOD =
    //                     Integer.parseInt(eElement.getElementsByTagName("system-check")
    //                             .item(0).getTextContent().trim());
                
    //             unitType = 
    //                     ((Element)eElement.getElementsByTagName("system-check").item(0)).getAttribute("unit");
    //             unit = parseUnit(unitType);
    //             Hdfs.Core.SYSTEM_CHECK_PERIOD *= unit;
                
    //             if (Hdfs.Core.SYSTEM_CHECK_PERIOD < 1) {
    //                 throw new ConfFormatException ("system check frequency cannot be non-positive");
    //             }
                
    //             /* parse NameNode IP Address */
    //             Hdfs.Core.NAME_NODE_IP =
    //                     eElement.getElementsByTagName("dataNodeip")
    //                             .item(0).getTextContent().trim();
                
    //             if (!ipValidator.isValidInet4Address(Hdfs.Core.NAME_NODE_IP)) {
    //                 throw new ConfFormatException("NameNode IP value is invalid");
    //             }
                
    //             /* parse NameNode port */
    //             Hdfs.Core.NAME_NODE_REGISTRY_PORT =
    //                     Integer.parseInt(eElement.getElementsByTagName("dataNodeport")
    //                             .item(0).getTextContent().trim());

                
    //             if (Hdfs.Core.NAME_NODE_REGISTRY_PORT  < 1024) {
    //                 throw new ConfFormatException ("NameNode port cannot use a well-known port.");
    //             }
    //             else if (Hdfs.Core.NAME_NODE_REGISTRY_PORT  > 65535){
    //                 throw new ConfFormatException (String.format("NameNode port(%d) cannot be larger than 65535.", Hdfs.Core.NAME_NODE_REGISTRY_PORT));
    //             }
                
    //         }
    //     }
    // }

}
