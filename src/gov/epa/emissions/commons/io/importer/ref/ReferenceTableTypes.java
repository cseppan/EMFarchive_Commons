package gov.epa.emissions.commons.io.importer.ref;

import gov.epa.emissions.commons.io.importer.TableType;
import gov.epa.emissions.commons.io.importer.TableTypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReferenceTableTypes implements TableTypes {

    private List list() {
        List list = new ArrayList();
        list.add(new TableType(ReferenceImporter.REFERENCE, ReferenceTable.types(), null));

        return list;
    }

    public TableType type(String datasetType) {
        for (Iterator iter = list().iterator(); iter.hasNext();) {
            TableType type = (TableType) iter.next();
            if (type.getDatasetType().equals(datasetType))
                return type;
        }

        return null;
    }

}
