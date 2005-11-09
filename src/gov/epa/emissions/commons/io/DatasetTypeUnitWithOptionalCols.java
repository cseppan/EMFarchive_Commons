package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.temporal.TableFormat;

public class DatasetTypeUnitWithOptionalCols implements FormatUnit{

    private TableFormatWithOptionalCols tableMetadata;

    private FileFormatWithOptionalCols fileMetadata;

    private boolean required;

    private InternalSource internalSource;

    public DatasetTypeUnitWithOptionalCols(TableFormatWithOptionalCols tableFormat,
            FileFormatWithOptionalCols fileFormat) {
        this.tableMetadata = tableFormat;
        this.fileMetadata = fileFormat;
        this.required = false;
    }

    public FileFormat fileFormat() {
        return fileMetadata;
    }

    public TableFormat tableFormat() {
        return tableMetadata;
    }
    
    public boolean isRequired(){
        return required;
    }
    
    public void setInternalSource(InternalSource internalSource){
        this.internalSource = internalSource;
    }
    
    public InternalSource getInternalSource(){
        return internalSource;
    }

}
