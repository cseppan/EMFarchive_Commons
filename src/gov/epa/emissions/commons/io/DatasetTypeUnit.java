package gov.epa.emissions.commons.io;

import java.io.File;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

public class DatasetTypeUnit implements FormatUnit {

    private TableFormat tableFormat;

    private FileFormat fileFormat;

    private boolean required;
    
    private File file;

    public DatasetTypeUnit(TableFormat tableFormat, FileFormat fileFormat) {
        this.tableFormat = tableFormat;
        this.fileFormat = fileFormat;
        this.required = false;
    }
    
    public DatasetTypeUnit(TableFormat tableFormat, FileFormat fileFormat, boolean required) {
        this.tableFormat = tableFormat;
        this.fileFormat = fileFormat;
        this.required = required;
    }

    public FileFormat fileFormat() {
        return fileFormat;
    }

    public TableFormat tableFormat() {
        return tableFormat;
    }
    
    public boolean isRequired(){
        return required;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }
    
    

}
