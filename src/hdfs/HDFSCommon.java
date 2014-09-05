package hdfs;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * An abstraction of files stored on DFS
 *
 */
public class HDFSCommon implements Serializable {

	/** default chunk size = 4KB */
	public final int DFT_CHUNK_SIZE = 4 * 1024;
	/** default file replica factor = 3 */
	public final int DFT_REPLICAS = 3;

}
