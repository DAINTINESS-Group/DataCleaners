package utils;

import java.io.File;
import java.io.Serializable;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import btree4j.utils.io.FileUtils;

public class BPlusTreeWrapper implements Serializable{
    
    private transient Database bPlusTree;
    private String dbName;

    public BPlusTreeWrapper(String dbName)
    {
        this.dbName = dbName;
    }

    public void initDatabase()
    {
        try
        { 
            File tmpFile = purgeDatabase();

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(false);
            
            Environment dbEnv = new Environment(tmpFile, envConfig);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);
            dbConfig.setTransactional(false);

            bPlusTree = dbEnv.openDatabase(null, dbName, dbConfig);
        }
        catch (Exception e)
        {
            System.out.println("XXX" + e);
        }
    }

    private File purgeDatabase()
    {
        File tempDir = FileUtils.getTempDir();
        File tmpFile = new File(tempDir, dbName);
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
