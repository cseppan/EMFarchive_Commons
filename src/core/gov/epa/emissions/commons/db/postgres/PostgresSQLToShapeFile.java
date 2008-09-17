package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.data.ProjectionShapeFile;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.ExporterException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PostgresSQLToShapeFile {

    Log log = LogFactory.getLog(PostgresSQLToShapeFile.class);

    private boolean windowsOS = false;
    
//    private DbServer dbServer;
    
    public PostgresSQLToShapeFile(DbServer dbServer) {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            windowsOS = true;
//        this.dbServer = dbServer;
    }

    public void create(String postgresBinDir, 
            String postgresDB, 
            String postgresUser, 
            String postgresPassword, 
            String filePath, 
            String selectQuery,
            ProjectionShapeFile projectionShapeFile) throws ExporterException {
        try {
            //make sure there is data for the shape file, if not throw an exception
            //dbServer
            
            createNewFile(filePath,
                    projectionShapeFile);

            String exportCommand = getWriteQueryString(postgresBinDir, 
                    postgresDB, 
                    postgresUser, 
                    postgresPassword,
                    filePath, 
                    selectQuery);

            Process process = Runtime.getRuntime().exec(exportCommand);
            //lets wait for the process to end, otherwise the process will run asynchronously,
            //and we swon't know when its finished...
            process.waitFor();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new ExporterException(e.getMessage());
        } finally {
            //
        }
    }

    private String putEscape(String path) {
        if (windowsOS)
            return path.replaceAll("\\\\", "\\\\\\\\");

        return path;
    }

    private String getWriteQueryString(String postgresBinDir, 
            String postgresDB, 
            String postgresUser, 
            String postgresPassword, 
            String filePath, 
            String selectQuery) {
        //"pgsql2shp -f test2 -P postgres -u postgres EMF "select * from us_state_shape"
        
        System.out.println("\"" + postgresBinDir + "pgsql2shp\" -f \"" + putEscape(filePath) + "\" -P " + postgresPassword + " -u " + postgresUser + " " + postgresDB + " \"" + selectQuery + "\"");
        
        return "\"" + postgresBinDir + "pgsql2shp\" -f \"" + putEscape(filePath) + "\" -P " + postgresPassword + " -u " + postgresUser + " " + postgresDB + " \"" + selectQuery + "\"";
    }

    private void createNewFile(String filePath,
            ProjectionShapeFile projectionShapeFile) throws Exception {
        try {
            System.out.println(filePath);
            // AME: Updates for EPA's system
            File dbfFile = new File(filePath + ".dbf");
            if (!dbfFile.exists()) {
                if (windowsOS) {
                    dbfFile.createNewFile();
                    Runtime.getRuntime().exec("CACLS " + dbfFile.getAbsolutePath() + " /E /G \"Users\":W");
                    dbfFile.setWritable(true, false);
                    Thread.sleep(1000); // for the system to refresh the file access permissions
                }
            } else {
                dbfFile.delete();
            }
            File shpFile = new File(filePath + ".shp");
            if (!shpFile.exists()) {
                if (windowsOS) {
                    shpFile.createNewFile();
                    Runtime.getRuntime().exec("CACLS " + shpFile.getAbsolutePath() + " /E /G \"Users\":W");
                    shpFile.setWritable(true, false);
                    Thread.sleep(1000); // for the system to refresh the file access permissions
                }
            } else {
                shpFile.delete();
            }
            File shxFile = new File(filePath + ".shx");
            if (!shxFile.exists()) {
                if (windowsOS) {
                    shxFile.createNewFile();
                    Runtime.getRuntime().exec("CACLS " + shxFile.getAbsolutePath() + " /E /G \"Users\":W");
                    shxFile.setWritable(true, false);
                    Thread.sleep(1000); // for the system to refresh the file access permissions
                }
            } else {
                shxFile.delete();
            }
            File prjFile = new File(filePath + ".prj");
            if (prjFile.exists()) prjFile.delete();
            prjFile.createNewFile();
            if (windowsOS) {
                Runtime.getRuntime().exec("CACLS " + prjFile.getAbsolutePath() + " /E /G \"Users\":W");
                prjFile.setWritable(true, false);
                Thread.sleep(1000); // for the system to refresh the file access permissions
            }
            Writer output = new BufferedWriter(new FileWriter(prjFile));
            try {
                output.write( projectionShapeFile.getPrjText() );
            }
            finally {
                output.close();
            }
            // for now, do nothing from Linux
        } catch (IOException e) {
            e.printStackTrace();
            throw new ExporterException("Could not create shape files: " + filePath);
        }
    }
}
