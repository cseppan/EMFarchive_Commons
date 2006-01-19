package gov.epa.emissions.commons.gui;

import javax.swing.JTable;
import javax.swing.table.TableModel;

public class EditableTable extends JTable implements Editor {

    public EditableTable(TableModel tableModel) {
        super(tableModel);
        setRowHeight(25);
    }

    public void commit() {
        if (isEditing()) {
            getCellEditor().stopCellEditing();
        }
    }

}
