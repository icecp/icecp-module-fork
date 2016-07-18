package com.intel.icecp.module.fork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intel.icecp.core.Attribute;
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Message;
import com.intel.icecp.core.Module;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.AttributeNotFoundException;
import com.intel.icecp.core.attributes.AttributeNotWriteableException;
import com.intel.icecp.core.attributes.AttributeRegistrationException;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.IdAttribute;
import com.intel.icecp.core.attributes.ModuleStateAttribute;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.module.fork.attributes.ForkedChannelsAttribute;
import com.intel.icecp.module.fork.attributes.IncomingChannelAttribute;
import com.intel.icecp.module.fork.attributes.MessageFilterAttribute;
import com.intel.icecp.node.AttributesFactory;
import com.intel.icecp.node.NodeFactory;
import com.intel.icecp.node.utils.ChannelUtils;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Fork Module
 *
 */
@SuppressWarnings("unchecked")
public class ForkModuleTest {
    private Node node;
    private Attributes attributes;
    private ForkModule forkModule;
    private URI uri;
    private URI incomingChannel;
    private URI testForkURI;

    @Mock
    private Attributes mockAttributes;
    @Mock
    private Node mockNode;
    @Mock
    private Channel<BytesMessage> mockResponseChannel;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);

        node = NodeFactory.buildMockNode();
        uri = ChannelUtils.join(node.getDefaultUri(), "/forkmodule");
        setupAttributes();
        forkModule = new ForkModule();

        incomingChannel = ChannelUtils.join(node.getDefaultUri(), "/test-fork");
        testForkURI = new URI("test-fork");
    }

    @Test
    public void testOpenChannelIsCalledOnceForSingleMessage() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<BytesMessage>)any(), any(Persistence.class))).thenReturn(mockResponseChannel);
        startMockForkModule();

        waitForFork();
        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message = createSampleMessage(sampleMessage);

        ForkModule.IncomingMessageCallback callback = forkModule.new IncomingMessageCallback(testForkURI, "$.sensoridentifier");
        callback.onPublish(message);
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSDEX_1")), (Class<BytesMessage>)any(), any(Persistence.class));
    }

    @Test
    public void testOpenChannelIsCalledOnceForTwoSameMessages() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<BytesMessage>)any(), any(Persistence.class))).thenReturn(mockResponseChannel);
        startMockForkModule();

        waitForFork();
        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message = createSampleMessage(sampleMessage);

        ForkModule.IncomingMessageCallback callback = forkModule.new IncomingMessageCallback(testForkURI, "$.sensoridentifier");
        callback.onPublish(message);

        callback.onPublish(message);
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSDEX_1")), (Class<BytesMessage>)any(), any(Persistence.class));
    }

    @Test
    public void testOpenChannelIsCalledTwiceForTwoDifferentMessages() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<BytesMessage>)any(), any(Persistence.class))).thenReturn(mockResponseChannel);
        startMockForkModule();

        waitForFork();
        String sampleMessage_1 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message_1 = createSampleMessage(sampleMessage_1);

        ForkModule.IncomingMessageCallback callback = forkModule.new IncomingMessageCallback(testForkURI, "$.sensoridentifier");
        callback.onPublish(message_1);

        String sampleMessage_2 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSTAP_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message_2 = createSampleMessage(sampleMessage_2);

        callback.onPublish(message_2);
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSDEX_1")), (Class<BytesMessage>)any(), any(Persistence.class));
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSTAP_1")), (Class<BytesMessage>)any(), any(Persistence.class));
    }

    @Test
    public void testOpenChannelIsCalledOnceEachWhenTwoSetsOfSameMessageIsPublished() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<BytesMessage>)any(), any(Persistence.class))).thenReturn(mockResponseChannel);
        startMockForkModule();

        waitForFork();
        String sampleMessage_1 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message_1 = createSampleMessage(sampleMessage_1);

        ForkModule.IncomingMessageCallback callback = forkModule.new IncomingMessageCallback(testForkURI, "$.sensoridentifier");
        // publish the first message
        callback.onPublish(message_1);

        String sampleMessage_2 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSTAP_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message_2 = createSampleMessage(sampleMessage_2);
        // publish the second message
        callback.onPublish(message_2);

        // publish the same first message again
        callback.onPublish(message_1);
        // publish the same second message again
        callback.onPublish(message_2);

        // verify the same forked channel is re-used even the same message is published twice
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSDEX_1")), (Class<BytesMessage>)any(), any(Persistence.class));
        verify(mockNode, times(1)).openChannel(eq(new URI("test-fork/SUNSETPASSTAP_1")), (Class<BytesMessage>)any(), any(Persistence.class));
    }

    @Test
    public void testAttributesAddThrowsException() throws Exception {
        doThrow(new AttributeRegistrationException("testMessage", new Exception())).when(mockAttributes).add(any(Attribute.class));
        forkModule.run(node, mockAttributes);

        assertFalse(mockAttributes.has(ForkedChannelsAttribute.class));
    }

    @Test
    public void testAttributesGetThrowsException() throws Exception {
        doThrow(new AttributeNotFoundException("testMessage")).when(mockAttributes).get(anyString(), any());
        forkModule.run(node, mockAttributes);

        assertFalse(mockAttributes.has(IncomingChannelAttribute.class));
    }

    @Test
    public void testAttributesSetThrowsException() throws Exception {
        when(mockAttributes.get(anyString(), any())).thenReturn("test-fork");
        doThrow(new AttributeNotWriteableException("testMessage")).when(mockAttributes).set(any(Class.class), any());
        forkModule.run(node, mockAttributes);

        assertFalse(mockAttributes.has(ModuleStateAttribute.NAME));
    }

    @Test
    public void testModuleStoppedSuccess() throws Exception {
        startForkModule();

        waitForFork();

        stopForkModule();
        assertEquals(Module.State.STOPPED, attributes.get(ModuleStateAttribute.NAME, Module.State.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenDeprecatedRunIsCalled() {
        forkModule.run(null, null, null, 0L);
    }

    @Test
    public void testForkedChannelAttributeCreatedSuccess() throws Exception {
        attributes.add(new MessageFilterAttribute(""));
        startForkModule();
        waitForFork();
        assertTrue(attributes.has(ForkedChannelsAttribute.class));
    }

    @Test
    public void testIfCallbackIsTriggeredAndMessageIsPublishedOnDefaultChannel() throws Exception {
        attributes.add(new MessageFilterAttribute(""));
        startForkModule();

        waitForFork();

        Channel<BytesMessage> incomingChannel = openChannel(this.incomingChannel);
        BytesMessage testMessage = new BytesMessage("...".getBytes());
        incomingChannel.publish(testMessage);

        assertNotNull(ForkModule.defaultChannel);
        assertArrayEquals(testMessage.getBytes(), ((BytesMessage) ForkModule.defaultChannel.latest().get()).getBytes());
    }

    @Test
    public void testSecondMessagePublishedDefaultChannelUsesSameInstanceOfChannel() throws Exception {
        attributes.add(new MessageFilterAttribute(""));
        startForkModule();

        waitForFork();

        Channel<BytesMessage> incomingChannel = openChannel(this.incomingChannel);
        incomingChannel.publish(new BytesMessage("msg_1".getBytes()));
        Channel<Message> defaultChannel_msg_1 = ForkModule.defaultChannel;

        BytesMessage testMessage_2 = new BytesMessage("msg_2".getBytes());
        incomingChannel.publish(testMessage_2);
        Channel<Message> defaultChannel_msg_2 = ForkModule.defaultChannel;

        assertNotNull(ForkModule.defaultChannel);
        assertEquals(defaultChannel_msg_1, defaultChannel_msg_2);
        assertArrayEquals(testMessage_2.getBytes(), ((BytesMessage) ForkModule.defaultChannel.latest().get()).getBytes());
    }

    @Test
    public void testMessageGotPublishedOnForkedChannel() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\",\"protocol\":{\"id\":1," +
                "\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\",\"type\":\"sensor\",\"value\":\"\"}";
        BytesMessage message = createSampleMessage(sampleMessage);

        Channel<BytesMessage> incomingChannel = openChannel(this.incomingChannel);
        incomingChannel.publish(message);

        URI testForkChannel = ChannelUtils.join(this.incomingChannel, "/SUNSETPASSDEX_1");
        Channel<BytesMessage> forkChannel = openChannel(testForkChannel);

        assertArrayEquals(message.getBytes(), forkChannel.latest().get().getBytes());
    }

    @Test
    public void testMessageUpdatesForkedChannelsAttributeSuccess() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage);

        Set<String> forkedChannelsSet = attributes.get(ForkedChannelsAttribute.FORKED_CHANNELS, Set.class);
        assertNotNull(forkedChannelsSet);

        String forkedChannel = incomingChannel + "/SUNSETPASSDEX_1";
        assertTrue(forkedChannelsSet.contains(forkedChannel));
    }

    @Test
    public void testPublishMultipleMessagesWithDifferentSensorsUpdatesForkedChannelsAttributeSucess() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage_1 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_1);

        String sampleMessage_2 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSTAP_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_2);

        Set<String> forkedChannelsSet = attributes.get(ForkedChannelsAttribute.FORKED_CHANNELS, Set.class);
        assertNotNull(forkedChannelsSet);

        assertEquals(2, forkedChannelsSet.size());
        assertTrue(forkedChannelsSet.contains(incomingChannel + "/SUNSETPASSDEX_1"));
        assertTrue(forkedChannelsSet.contains(incomingChannel + "/SUNSETPASSTAP_1"));
    }

    @Test
    public void testPublishMultipleMessagesWithSameSensorsHasOnlyOneValue() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage_1 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_1);

        String sampleMessage_2 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_2);

        Set<String> forkedChannelsSet = attributes.get(ForkedChannelsAttribute.FORKED_CHANNELS, Set.class);
        assertNotNull(forkedChannelsSet);

        assertEquals(1, forkedChannelsSet.size());
        assertTrue(forkedChannelsSet.contains(incomingChannel + "/SUNSETPASSDEX_1"));
    }

    @Test
    public void testModuleClosesAllChannelsGracefully() throws Exception {
        attributes.add(new MessageFilterAttribute(""));

        startForkModule();

        waitForFork();

        String sampleMessage_1 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_1);

        String sampleMessage_2 = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"SUNSETPASSDEX_1\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage_2);

        stopForkModule();

        waitForFork();

        assertFalse(ForkModule.defaultChannel.isOpen());
    }

    @Test
    public void testMissingSensorIdInPayloadThrowsException() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31}," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage);

        URI testForkChannel = ChannelUtils.join(incomingChannel, "/");
        Channel<BytesMessage> forkChannel = openChannel(testForkChannel);
        try {
            forkChannel.latest().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ChannelIOException);
        }
    }

    @Test
    public void testEmptySensorIdInMessageDoesNotGetPublishedThrowsException() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage);

        URI testForkChannel = ChannelUtils.join(incomingChannel, "/");
        Channel<BytesMessage> forkChannel = openChannel(testForkChannel);
        try {
            forkChannel.latest().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ChannelIOException);
        }
    }

    @Test
    public void testEmptySensorIdInMessageDoesNotGetUpdateAttribute() throws Exception {
        attributes.add(new MessageFilterAttribute("$.sensoridentifier"));

        startForkModule();

        waitForFork();

        String sampleMessage = "{\"datetime\":\"2015-11-01T17:57:53-0700\",\"deviceidentifier\":\"00137a0018cdd\"," +
                "\"protocol\":{\"id\":1,\"name\":\"SunsetPassDEX\",\"type\":31},\"sensoridentifier\":\"\"," +
                "\"type\":\"sensor\",\"value\":\"\"}";
        createAndPublishMessage(sampleMessage);

        Set<String> forkedChannelsSet = attributes.get(ForkedChannelsAttribute.FORKED_CHANNELS, Set.class);
        assertNull(forkedChannelsSet);
    }

    private void createAndPublishMessage(String message) throws Exception {
        BytesMessage sampleMessage = createSampleMessage(message);

        Channel<BytesMessage> incomingChannel = openChannel(this.incomingChannel);
        incomingChannel.publish(sampleMessage);
    }

    private Channel<BytesMessage> openChannel(URI channel) throws com.intel.icecp.core.misc.ChannelLifetimeException {
        return node.openChannel(channel, BytesMessage.class, new Persistence());
    }

    private BytesMessage createSampleMessage(String message) throws Exception {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(message.getBytes());

        ObjectMapper mapper = new ObjectMapper();
        return new BytesMessage(mapper.writeValueAsBytes(mqttMessage));
    }

    private void stopForkModule() {
        forkModule.stop(Module.StopReason.USER_DIRECTED);
    }

    private void setupAttributes() throws Exception {
        attributes = AttributesFactory.buildEmptyAttributes(node.channels(), uri);
        attributes.add(new IdAttribute(99));
        attributes.add(new ModuleStateAttribute());
        attributes.add(new IncomingChannelAttribute("/test-fork"));
    }

    private void waitForFork() {
        // use latch to await the specified time
        CountDownLatch latch = new CountDownLatch(1);
        try {
            latch.await(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignored
        }
    }

    private void startForkModule() {
        new Thread(() -> forkModule.run(node, attributes)).start();
    }

    private void startMockForkModule() {
        new Thread(() -> forkModule.run(mockNode, mockAttributes)).start();
    }
}