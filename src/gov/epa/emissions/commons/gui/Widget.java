package gov.epa.emissions.commons.gui;

import java.awt.event.KeyListener;

import javax.swing.JComponent;

public interface Widget {
    JComponent element();

    String value();

    void addKeyListener(KeyListener listener);
}
