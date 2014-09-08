package hdfs;

import java.io.Serializable;
import java.util.Date;

/**
 * HDFS files MetaData
 *
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class HDFSFileMeta implements Serializable {
    
    /** serialVersionUID */
    private static final long serialVersionUID = -6302186159396021997L;
    /** file name */
    private String name;
    /** fiel size */
    private int size;
    /** modification time */
    private Date modTime;
    

    /** Constructor */
    public HDFSFileMeta(String name, int size) {
        this.name = name;
        this.size = size;
    }

    /** get file name */
    String getName() {
        return this.name;
    }

    /** get file size */
    int getSize() {
        return this.size;
    }

    /** get file size */
    void setSize(int size) {
        this.size = size;
    }

    /** get file size */
    Date getModTime() {
        return this.modTime;
    }
}

