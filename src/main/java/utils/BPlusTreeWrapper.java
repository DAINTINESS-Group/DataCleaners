package utils;

import java.io.File;
import java.io.Serializable;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import btree4j.utils.io.FileUtils;

/**
 * This helper class represents a single database that uttilises Berkley DB's BPlusTree structures to hold key,value
 * pairs in the disk.
 */
public class BPlusTreeWrapper implements Serializable{
    
    private static final long serialVersionUID = 10561941102347773L;
	private transient Database bPlusTree;
    private String dbName;

    public BPlusTreeWrapper(String dbName)
    {
        this.dbName = dbName;
    }

    public void initDatabase(boolean purgePreviousDatabase)
    {
        try
        { 
            File tmpFile = getDatabaseFile(purgePreviousDatabase);

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(false);
            
            Environment dbEnv = new Environment(tmpFile, envConfig);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(false);
            dbConfig.setTransactional(false);

            bPlusTree = dbEnv.openDatabase(null, dbName, dbConfig);
        }
        catch (Exception e)
        {
            System.out.println("XXX" + e);
        }
    }

    private File getDatabaseFile(boolean purgePreviousDatabase)
    {
        File tempDir = FileUtils.getTempDir();
        File tmpFile = new File(tempDir, dbName);
        if (!purgePreviousDatabase) return tmpFile;

        if (tmpFile.exists())
        {   
            for (File f : tmpFile.listFiles())
            {
                f.delete();
            }
            tmpFile.delete(); 
        }
        tmpFile.mkdir();
        return tmpFile;
    }

    public Database getBPlusTree() { return bPlusTree; }
}
