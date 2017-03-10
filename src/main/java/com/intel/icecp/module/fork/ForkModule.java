/*
 * Copyright (c) 2016 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.icecp.module.fork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Message;
import com.intel.icecp.core.Module;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.AttributeNotFoundException;
import com.intel.icecp.core.attributes.AttributeNotWriteableException;
import com.intel.icecp.core.attributes.AttributeRegistrationException;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.ModuleStateAttribute;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.core.misc.Configuration;
import com.intel.icecp.core.misc.OnPublish;
import com.intel.icecp.core.modules.ModuleProperty;
import com.intel.icecp.module.fork.attributes.ForkedChannelsAttribute;
import com.intel.icecp.module.fork.attributes.IncomingChannelAttribute;
import com.intel.icecp.module.fork.attributes.MessageFilterAttribute;
import com.intel.icecp.node.utils.ChannelUtils;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Fork module that shall fork an incoming channel into multiple channels based on {@link MessageFilterAttribute} in the
 * incoming message.
 * It shall take an incoming channel URI as configuration and exposes a {@link IncomingChannelAttribute} attribute
 * and expose resulting channels (channel URIs) externally through the {@link ForkedChannelsAttribute} attribute.
 * The forked channels are of the format {@code <incomingChannel URI>/<extracted value from the messageFilter>}
 *
 * <p>
 * Ex: If the messageFilter is {@code $.sensoridentifier} and incomingChannel is {@code ndn:/test-fork/} and the
 *     incoming message looked like: {@code {"timestamp":..., "sensoridentifier":"sensorId1234"}}
 *     The resulting forked channel will have the format {@code ndn:/test-fork/sensorId1234}
 *
 */
@ModuleProperty(name = "ForkModule", attributes = {IncomingChannelAttribute.class, MessageFilterAttribute.class, ForkedChannelsAttribute.class})
public class ForkModule implements Module {
    private static final Logger LOGGER = LogManager.getLogger();
    // default channel to publish messages on, if no message-filter has been configured
    private static final String DEFAULT_FORKED_CHANNEL_NAME = "/DEFAULT-DATA";
    static Channel<Message> defaultChannel;
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private Node node;
    private Channel<BytesMessage> incomingDataChannel;
    private Attributes attributes;
    private ForkedChannelsAttribute forkedChannelAttribute;
    // set holding all the newly created channels
    private final Set<String> forkChannelSet;
    // internal map for maintaining the channels and closing them during module unload
    private final Map<String, Channel<Message>> channels;

    /**
     * Default constructor
     *
     */
    public ForkModule() {
        forkChannelSet = new TreeSet<>();
        channels = new HashMap<>();
    }

    /**
     * @deprecated use {@link #run(Node, Attributes)} instead.
     *
     */
    @Override
    @Deprecated
    public void run(Node node, Configuration moduleConfiguration, Channel<State> moduleStateChannel, long moduleId) {
        throw new UnsupportedOperationException("Deprecated version of run, will be removed entirely in a future release");
    }

    @Override
    public void run(Node node, Attributes attributes) {
        this.node = node;
        this.attributes = attributes;

        try {
            // register the forked channel attribute
            this.forkedChannelAttribute = new ForkedChannelsAttribute();
            this.attributes.add(forkedChannelAttribute);

            // open incoming data channel
            final URI incomingChannel = ChannelUtils.join(node.getDefaultUri(), attributes.get(IncomingChannelAttribute.INCOMING_CHANNEL, String.class));
            LOGGER.info("Incoming channel name is: {}", incomingChannel);
            incomingDataChannel = node.openChannel(incomingChannel, BytesMessage.class,
                    new Persistence());

            // set up callback for incoming messages and fetch message-filter attribute
            LOGGER.info("Set up callback for: {}", incomingChannel);
            final String messageFilter = attributes.get(MessageFilterAttribute.MESSAGE_FILTER, String.class);
            IncomingMessageCallback incomingMessageCallback = new IncomingMessageCallback(incomingChannel, messageFilter);
            incomingDataChannel.subscribe(incomingMessageCallback);
            LOGGER.info("Callback setup success. Channel {} is now waiting for messages", incomingChannel);

            // set module state to RUNNING
            attributes.set(ModuleStateAttribute.class, State.RUNNING);
            LOGGER.info("Set state of module to {}", State.RUNNING.name());

            waitForTearDown();
        } catch (ChannelIOException | ChannelLifetimeException e) {
            LOGGER.error("Error opening splitter data channel", e);
            setAttribute(ModuleStateAttribute.class, State.ERROR);
        } catch (AttributeNotFoundException | AttributeNotWriteableException | AttributeRegistrationException e) {
            LOGGER.error("Error on attributes", e);
        } finally {
            closeAllChannels();
        }
    }

    private void closeAllChannels() {
        closeAllForkedChannels();
        closeChannel(defaultChannel);
        closeChannel(incomingDataChannel);
    }

    /**
     * Private method to close all forked-channels
     */
    private void closeAllForkedChannels() {
        if (!channels.isEmpty())
            channels.values().parallelStream().forEach(ForkModule::closeChannel);
    }

    /**
     * Method to close channel
     *
     * @param channel the channel instance to close
     */
    private static void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (ChannelLifetimeException e) {
                LOGGER.error("Failed to close channel: {}", channel.getName(), e);
            }
        }
    }

    /**
     * Fork callback class which receives all the Retail specific messages from icecp-module-mqtt
     *
     */
    class IncomingMessageCallback implements OnPublish<BytesMessage> {
        private final AtomicInteger counter = new AtomicInteger(0);
        private final URI incomingChannel;
        private final String messageFilter;


        /**
         * Constructor with attributes
         *
         * @param messageFilter message filter string
         */
        IncomingMessageCallback(final URI incomingChannel, final String messageFilter) {
            this.incomingChannel = incomingChannel;
            this.messageFilter = messageFilter;
        }

        @Override
        public void onPublish(BytesMessage message) {
            int id = counter.incrementAndGet();
            LOGGER.info("ID: {}, Message received = {} bytes", id, message.getBytes().length);
            if (messageFilter != null && messageFilter.length() > 0) {
                // convert the BytesMessage into a MqttMessage
                ObjectMapper mapper = new ObjectMapper();
                String forkChannelName = null;
                try {
                    MqttMessage mqttMessage = mapper.readValue(new String(message.getBytes()), MqttMessage.class);
                    LOGGER.debug("ID: {}, Conversion to MQTT message complete", id);

                    // get the sensorId fields from the payload by applying message-filter
                    String channelSuffix = JsonPath.read(new String(mqttMessage.getPayload()), messageFilter);
                    LOGGER.debug("ID: {}, channelSuffix: {} from payLoad", id, channelSuffix);

                    // create new channel if already not exists, and then publish the message on the channel
                    if (channelSuffix != null && channelSuffix.length() > 0) {
                        // construct forked channel URI
                        forkChannelName = incomingChannel + "/" + channelSuffix;
                        Channel<Message> forkChannel = getMessageChannel(forkChannelName);
                        forkChannel.publish(message);

                        forkChannelSet.add(forkChannelName);
                        // update the attribute with the updated set
                        forkedChannelAttribute.value(forkChannelSet);
                        channels.put(forkChannelName, forkChannel);
                    } else {
                        LOGGER.info("MQTTMessage payload do not contain identifier, filter: {} failed! ", messageFilter);
                    }
                } catch (IOException e) {
                    LOGGER.error("ID: {}, Failed to convert to MQTT message", id, e);
                } catch (ChannelLifetimeException | URISyntaxException e) {
                    LOGGER.error("ID: {}, Failed to open channel with name {}", id, forkChannelName, e);
                } catch (ChannelIOException e) {
                    LOGGER.error("ID: {}, Failed to publish message to the channel!", id, e);
                } catch (PathNotFoundException e) {
                    LOGGER.error("Missing messageFilter: {} in message: {}", messageFilter, message, e);
                }
            } else {
                LOGGER.info("No message-filter found, publishing on default channel: {}", DEFAULT_FORKED_CHANNEL_NAME);
                publishOnDefaultChannel(message);
            }
        }

        /**
         * Private method to check if channel already exists, else open a new one
         * @param forkChannelName name of the forked channel
         * @return old forked channel if it already exists, else newly created channel
         * @throws ChannelLifetimeException failure to open channel
         * @throws URISyntaxException incorrect URI of the forked channel
         */
        private Channel<Message> getMessageChannel(String forkChannelName) throws ChannelLifetimeException, URISyntaxException {
           return channels.containsKey(forkChannelName) ? channels.get(forkChannelName)
                   : node.openChannel(new URI(forkChannelName), Message.class, new Persistence());
        }

        /**
         * Private method to publish messages on a default channel
         * @param message message to be published
         */
        private synchronized void publishOnDefaultChannel(BytesMessage message) {
            String defaultChannelName = node.getDefaultUri() + DEFAULT_FORKED_CHANNEL_NAME;
            try {
                if (defaultChannel == null) {
                    defaultChannel = node.openChannel(new URI(defaultChannelName), Message.class, new Persistence());
                }
                defaultChannel.publish(message);
            } catch (ChannelIOException | ChannelLifetimeException | URISyntaxException e) {
                LOGGER.error("Failed to publish on default channel: {}", defaultChannelName, e);
            }
        }
    }

    /**
     * Set an attribute with class and value pair with error handling
     *
     * @param attributeClass  class of the attribute to be set
     * @param attributeValue value of the attribute to be set
     */
    private void setAttribute(Class attributeClass, Object attributeValue) {
        try {
            attributes.set(attributeClass, attributeValue);
        } catch (AttributeNotFoundException e) {
            LOGGER.error("Attribute {} not set, please check config file, {}", attributeClass.getName(), e);
        } catch (AttributeNotWriteableException e) {
            LOGGER.error("Attribute {} not writable", attributeClass.getName(), e);
        }
    }

    /**
     * Wait for stopLatch to turn to 0 which will indicate the application should terminate. See the stop() method.
     */
    private void waitForTearDown() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted.", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop(StopReason reason) {
        LOGGER.info("Module stopped: {}", reason);
        setAttribute(ModuleStateAttribute.class, State.STOPPED);
        stopLatch.countDown();
    }

}