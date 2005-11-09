package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.DatasetType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReferenceTableTypes {

    public DatasetType reference() {
        return new DatasetType("Reference");
    }

    private List list() {
        List list = new ArrayList();
        list.add(new TableType(reference(), ReferenceTable.types(), null));

        return list;
    }

    public TableType type(DatasetType datasetType) {
        for (Iterator iter = list().iterator(); iter.hasNext();) {
            TableType type = (TableType) iter.next();
            if (type.datasetType().equals(datasetType))
                return type;
        }

        return null;
    }

}
