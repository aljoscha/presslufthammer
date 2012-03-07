package de.tuberlin.dima.presslufthammer.transport.messages;

public class TabletMessage {
    private final int queryId;
    private final byte[] tabletData;
    
    public TabletMessage(int queryId, byte[] tabletData) {
        super();
        this.queryId = queryId;
        this.tabletData = tabletData;
    }

    public int getQueryId() {
        return queryId;
    }

    public byte[] getTabletData() {
        return tabletData;
    }
}
