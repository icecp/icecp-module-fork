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

package com.intel.icecp.module.fork.attributes;

import com.intel.icecp.core.attributes.WriteableBaseAttribute;

/**
 * Used for creating a message-filter attribute {@code MESSAGE_FILTER} which defines the matching criteria for messages
 * in order to fork into multiple channels.  This attribute expects a valid JsonPath expression.
 * Eg: If the user wants to match criteria to be sensorIdentifier, then the content of JSON configuration should contain this
 * {@code "message-filter" : "$.sensoridentifier" }
 *
 */
public class MessageFilterAttribute extends WriteableBaseAttribute<String> {
    public static final String MESSAGE_FILTER = "message-filter";
    /**
     * Constructor to create the messageFilter attribute with no value
     */
    public MessageFilterAttribute() {
        super(MESSAGE_FILTER, String.class);
    }

    /**
     * Constructor to create the messageFilter attribute with a value
     * @param attributeValue value of the attribute
     */
    public MessageFilterAttribute(String attributeValue) {
        this();
        value(attributeValue);
    }
}
