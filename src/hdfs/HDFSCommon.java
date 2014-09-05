package hdfs;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * HDFS global parameters
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSCommon implements Serializable {

	/** default chunk size = 4KB */
	public final int DFT_CHUNK_SIZE = 4 * 1024;
	/** default file replica factor = 3 */
	public final int DFT_REPLICAS = 3;

}

