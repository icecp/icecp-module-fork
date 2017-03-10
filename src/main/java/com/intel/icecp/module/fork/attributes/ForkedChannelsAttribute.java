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

import java.util.Set;

/**
 * Used for creating a forked-channels list attribute {@code FORKED_CHANNELS} to output the list of forked channels
 *
 */
public class ForkedChannelsAttribute extends WriteableBaseAttribute<Set<String>> {
    public static final String FORKED_CHANNELS = "forked-channels";
    private Set<String> value;

    /**
     * Constructor to create the forkedChannels attribute with no value
     */
    public ForkedChannelsAttribute() {
        super(FORKED_CHANNELS, Set.class);
    }

    @Override
    public void value(Set<String> newValue) {
       value = newValue;
    }

    @Override
    public Set<String> value() {
        return value;
    }
}
