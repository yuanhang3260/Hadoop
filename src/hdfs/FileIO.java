package hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * This class is a utility class for DFS I/O. This class provide the following
 * several methods : writeFile(String s), readFile(String filename),
 * writeBinary(byte[] bytes), readBinary(String filename), writeObject(Object
 * object), readConfig(String path)
 * 
 * @author Hang Yuan
 * @author Chuhan Yang
 */
public class FileIO {
    
    /**
     * Read a file into 
     * 
     * @param filename String The file you need to read
     * @return byte[] The return type is byte array
     * @throws IOException
     */
    public static byte[] readFile(String filename) throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            System.err.println("[Error**] File " + filename + " does not exist!");
            return null;
        }

        FileInputStream fis = null;
        byte[] content = new byte[(int)file.length()];
        try {
            fis = new FileInputStream(file);
            fis.read(content);
        }
        catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        }
        catch (IOException e) {
            throw new IOException(e.toString());
        }
        finally {
            try {
                fis.close();
            } catch (IOException e) {
                throw new IOException(e.toString());
            }
        }
        return content;
    }


    /**
     * This method is used to read the configuration file and fill the values
     * into the correlated field of the Object
     * 
     * @param filename the configuration file name you want to read
     * @param obj the Object you want to fill
     * @throws IOException 
     */
    public static void readConf(String filename, Object obj) throws IOException {
        String content = null;
        try {
            content = new String(readFile(filename), "UTF-8");
        }
        catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        }
        catch (IOException e) {
            throw new IOException(e.toString());
        }

        String[] lines = content.trim().split("\n");
        for (String line : lines) {
            String temp[] = new String[2];
            int position = line.lastIndexOf("=");
            if (position == -1) {
                continue;
            }
            temp[0] = line.substring(0, position);
            temp[1] = line.substring(position + 1, line.length());
            try {
                Field field = obj.getClass().getDeclaredField(temp[0]);
                field.setAccessible(true);
                if (field.getType().isPrimitive()) {
                    field.setInt(obj, Integer.parseInt(temp[1]));
                }
                else if (field.getType().equals(String.class)) {
                    field.set(obj, temp[1]);
                }
                else if (field.getType().equals(Integer.class)) {
                    field.set(obj, Integer.parseInt(temp[1]));
                }
                else if (field.getType().equals(Double.class)) {
                    field.set(obj, Double.parseDouble(temp[1]));
                }
            }
            catch (NoSuchFieldException e) {
            	/* if no such field, continue */
                continue;
            }
            catch (SecurityException e) {
                throw new IOException(e.toString());
            }
            catch (NumberFormatException e) {
                continue;
            }
            catch (IllegalArgumentException e) {
                throw new IOException(e.toString());
            }
            catch (IllegalAccessException e) {
                throw new IOException(e.toString());
            }
        }
    }


    /**
     * Write to file from buffer
     * 
     * @param buf byte[] src content buffer
     * @param filename target file name
     * @throws IOException 
     */
    public static void writeFile(byte[] buf, String filename) throws IOException {
        int index = filename.length() - 1;
        while(index >= 0 && filename.charAt(index) != '/') {
            index--;
        }
        String dir = filename.substring(0, index);
        
        /* create dir if necessary */
        File fileDir = new File(dir);
        if(!fileDir.exists()) {
            System.out.println("create dir: " + dir);
            fileDir.mkdirs();
        }
        
        /* if file does not exist, create it */
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IOException(e.toString());
            }
        }

        /* write file */
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write(buf);
        }
        catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        }
        catch (IOException e) {
            throw new IOException(e.toString());
        }
        finally {
            try {
                fos.close();
            }
            catch (IOException e) {
                throw new IOException(e.toString());
            }
        }
    }


    /**
     * Overload method : write to file a from a string
     * 
     * @param content String the contents we want to write into the file
     * @param filename target file name
     * @throws IOException 
     */
    public static void writeFile(String content, String filename) throws IOException {
        byte[] buf = content.getBytes();
        writeFile(buf, filename);
        return;
    }


    /**
     * Remove a file from local storage.
     * @param filePath String File path to be deleted.
     * @throws IOException
     */
    public static void deleteFile(String filePath) throws IOException{
        File file = new File(filePath);
        if (!file.exists()) {
	        file.delete();
        }
        else {
        	System.err.println("[Error**] No file " + filePath + " found");
        }
        return;
    }

    



    //////////////////////////////////////////// TODO ////////////////////////////////////////////////
    /**
     * Read a chunk from a local file.
     * @param file RandomAccessFile The file to be read.
     * @param startPosition Long The starting point of this chunk.
     * @param size int The size of this chunk. 
     * @return bytep[] Content of this chunk.
     * @throws IOException
     */
    public static byte[] readChunk(RandomAccessFile file, long startPosition, int size) throws IOException {
        byte tmp = -1;
        byte[] chunk = new byte[size];
        int index = 0;
        file.seek(startPosition);
        
        try {
            while (index < size && (tmp = file.readByte()) != -1) {
                chunk[index++] = tmp;
            }
        } catch (EOFException e) {
        }
        return chunk;
    }


    /**
     * Append chunks into one file.
     * @param filePath String The path of file to be appended to.
     * @param content byte[] The content of chunks to be appended.
     * @throws IOException
     */
    public static void appendBytesToFile(String filePath, byte[] content) throws IOException {
        int index = filePath.length() - 1;
        while(index >= 0 && filePath.charAt(index) != '/') {
            index--;
        }
        String dir = filePath.substring(0, index);
        
        File fileDir = new File(dir);
        if(!fileDir.exists()) {
            System.out.println("create dir: " + dir);
            fileDir.mkdirs();
        }
        
        File file = new File(filePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IOException(e.toString());
            }
        }
        
        try {
            FileWriter out = null;
            out = new FileWriter(file, true);
            for (byte b : content)
                out.append((char) b);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    
    /**
     * Calculate how many splits are there for a specific file. It contains the starting point and EOF.
     * @param filePath String The path of the file.
     * @param chunkSize String The chunk size to be used.
     * @return ArrayList<Long> An array indicates all the offsets of start points of chunks.
     * @throws IOException
     */
    public static ArrayList<Long> calculateFileSplit(String filePath, int chunkSize) throws IOException {
        RandomAccessFile raFile = null;
        ArrayList<Long> split = new ArrayList<Long>();
        try {
            raFile = new RandomAccessFile(filePath, "r");
            long currentPointer = 0L;
            long fileSize = raFile.length();
            String tmp = null;
            Long lastPointer = 0L;
            split.add(0L);
            System.out.println("Start scanning file...");
            do {
                tmp = raFile.readLine();
                if (tmp != null) {
                    tmp += '\n';
                    int increment = tmp.getBytes().length;
                    if (increment > chunkSize) {
                        throw new IOException("Data row is too long...");
                    }
                    if (currentPointer - lastPointer + increment <= chunkSize) { 
                        currentPointer += increment;
                    } else {
                        split.add(currentPointer);            //starting point of next chunk
                        System.out.println("Scanning file... " + (int)((1.0d * currentPointer / fileSize) * 100) + "% finished.");
                        lastPointer = currentPointer;
                        currentPointer += increment;
                    }
                } else {    //reach the end of file
                    if (tmp == null && currentPointer != lastPointer) {
                        split.add(currentPointer);
                        System.out.println("Finished scanning file.");
                    }
                }
            } while (tmp != null);
        } catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        } catch (IOException e) {
            throw new IOException(e.toString());
        } finally {
            try {
                raFile.close();
            } catch (IOException e) {
                throw new IOException(e.toString());
            }
        }
        return split;
    }
}