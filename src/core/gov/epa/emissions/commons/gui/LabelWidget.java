package gov.epa.emissions.commons.gui;

import java.awt.event.KeyListener;

import javax.swing.JComponent;
import javax.swing.JLabel;

public class LabelWidget implements Widget {

    private JLabel label;

    public LabelWidget(String name, String value) {
        label = new JLabel(value);
        label.setName(name);
    }

    public JComponent element() {
        return label;
    }

    public String value() {
        return label.getText();
    }

    public void addKeyListener(KeyListener listener) {
        // ignored. Not Applicable
    }

}
