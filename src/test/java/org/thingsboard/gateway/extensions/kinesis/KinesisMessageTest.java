package org.thingsboard.gateway.extensions.kinesis;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.thingsboard.gateway.extensions.kinesis.KinesisMessage;



public class KinesisMessageTest {

    @Test
    public void shouldCreateKinesisMessage() {
        KinesisMessage message = new KinesisMessage();

        assertNotNull(message);

        verifyInitialParameters(message);
    }


    private void verifyInitialParameters(KinesisMessage message) {
        assertNull(message.path);
        assertNull(message.value);
        assertNull(message.type);
        assertNull(message.cls);

        assertTrue(message.timestamp == 0L);
        assertTrue(message.tzoffset == 0);

        assertNull(message.analyticsId);
        assertNull(message.uniqueId);
        assertNull(message.oid);
        assertNull(message.name);
    }
}
