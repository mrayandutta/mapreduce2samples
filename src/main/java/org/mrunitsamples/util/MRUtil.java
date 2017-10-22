package org.mrunitsamples.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Properties;

public class MRUtil {
    private static Properties props = null;

    public static final String getValueByKey(String key)
    {
        if(props == null)
        {
            props = new Properties();
            try
            {
                props.load(MRUtil.class.getResourceAsStream("/mr.properties"));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        String value = props.getProperty(key);
        return value;

    }

    public static Configuration getLocalConfiguration(Configuration conf)
    {
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        //conf.set("fs.file.impl",WindowsLocalFileSystem.class.getName());
        conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"+ "org.apache.hadoop.io.serializer.WritableSerialization");
        return conf;
    }

    public static Configuration getRemoteConfiguration(Configuration conf)
    {
        conf.set("fs.default.name", MRUtil.getValueByKey("remote.namenode.url"));
        conf.set("mapred.job.tracker", MRUtil.getValueByKey("remote.jobtracker.url"));
        return conf;
    }

    public static void deleteDir(String folderName) throws IOException {
        Configuration conf = MRUtil.getRemoteConfiguration(new Configuration());
        FileSystem fileSystem = FileSystem.get(conf);
        Path newFolderPath = new Path(folderName);

        if (fileSystem.exists(newFolderPath)) {
            System.out.println("Folder " + folderName
                    + " exists ,going to delete ..");
            fileSystem.delete(newFolderPath, true); // Delete existing Directory
        } else {
            System.out.println("Folder " + folderName + " does not exist.");
        }

    }



    public static void main(String[] args) {
        System.out.println(getValueByKey("IP"));
    }

}
