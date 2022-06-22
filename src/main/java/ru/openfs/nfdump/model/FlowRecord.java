package ru.openfs.nfdump.model;

public class FlowRecord {
    final long last;
    final String subnet;
    final int proto;
    final int local_ip;
    final int local_port;
    final int remote_ip;
    final int remote_port;
    final int asn;
    int uplink;
    int downlink;
    long bytesIn;
    long bytesOut;

    public FlowRecord(long last, String subnet, int proto, int asn,
            int local_ip, int local_port, int remote_ip, int remote_port,
            long bytesIn, long bytesOut, int uplink, int downlink) {
        this.last = last;
        this.subnet = subnet;
        this.asn = asn;
        this.proto = proto;
        this.local_ip = local_ip;
        this.local_port = local_port;
        this.remote_ip = remote_ip;
        this.remote_port = remote_port;
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
        this.uplink = uplink;
        this.downlink = downlink;
    }

    public long getLast() {
        return this.last;
    }

    public int getProto() {
        return this.proto;
    }

    public String getSubnet() {
        return this.subnet;
    }

    public int getLocalIp() {
        return this.local_ip;
    }

    public int getLocalPort() {
        return this.local_port;
    }

    public int getRemoteIp() {
        return this.remote_ip;
    }

    public int getRemotePort() {
        return this.remote_port;
    }

    public int getUplink() {
        return this.uplink;
    }

    public int getDownlink() {
        return this.downlink;
    }

    public int getAsn() {
        return this.asn;
    }

    public Long getBytesIn() {
        return this.bytesIn;
    }

    public Long getBytesOut() {
        return this.bytesOut;
    }

    public FlowRecord appendAndGet(FlowRecord record) {
        this.bytesIn += record.getBytesIn();
        this.bytesOut += record.getBytesOut();
        if (record.getUplink() != 0)
            this.uplink = record.getUplink();
        if (record.getDownlink() != 0)
            this.downlink = record.getDownlink();
        return this;
    }

}
