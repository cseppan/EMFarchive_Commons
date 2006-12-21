package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class InventoryTableExporter extends GenericExporter {
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), optimizedBatchSize);
        setDelimiter("");
    }
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), factory, optimizedBatchSize);
        setDelimiter("");
    } 
    
    protected int startCol(String[] cols) {
        if (isTableVersioned(cols))
            return 6; //shifted by "Obj_Id", "Record_Id", 
                      //"Dataset_Id", "Version", "Delete_Versions", "Line_Number"

        return 3; //shifted by "Obj_Id", "Record_Id", "Line_Number"
    }
}
