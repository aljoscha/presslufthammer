package de.tuberlin.dima.presslufthammer.util;

public class ShutdownStopper extends Thread {
    private Stoppable stoppable;

    public ShutdownStopper(Stoppable stoppable) {
        this.stoppable = stoppable;
    }

    public void run() {
        stoppable.stop();
    }
}
