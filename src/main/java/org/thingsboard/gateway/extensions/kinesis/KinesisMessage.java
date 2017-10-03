package org.thingsboard.gateway.extensions.kinesis;

public class KinesisMessage {

    public String path;
    public String value;
    public String type;
    public String cls;
    public String from;
    public long timestamp;
    public int tzoffset;
    public String analyticsId;
    public String uniqueId;
    public String oid;
}