package gov.epa.emissions.commons.gui;

import javax.swing.JTextField;

public class TextField extends JTextField {

    public TextField(String name, int size) {
        super(size);
        super.setName(name);
    }

    public TextField(String name, String value, int size) {
        this(name, size);
        super.setText(value);
    }
}
