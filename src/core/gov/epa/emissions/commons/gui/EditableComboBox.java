package gov.epa.emissions.commons.gui;

import javax.swing.JComboBox;

public class EditableComboBox extends JComboBox{
    
    public EditableComboBox(Object[] objects) {
        super(objects);
        setEditable(true);
    }
}
