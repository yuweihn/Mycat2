/**
 * Copyright (C) <2021>  <Hash Zhang>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.util.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ConcurrentModificationException;

/**
 * Selector工具类
 * Created by Hash Zhang on 2017/7/24.
 */
public class SelectorUtil {

    public static final int REBUILD_COUNT_THRESHOLD = 512;
    public static final long MIN_SELECT_TIME_IN_NANO_SECONDS = 500000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorUtil.class);

    public static Selector rebuildSelector(final Selector oldSelector) throws IOException {
        final Selector newSelector;
        try {
            newSelector = Selector.open();
        } catch (Exception e) {
            LOGGER.warn("Failed to create a new Selector.", e);
            return null;
        }

        int nChannels = 0;
        for (; ; ) {
            try {
                for (SelectionKey key : oldSelector.keys()) {
                    Object a = key.attachment();
                    try {
                        if (!key.isValid() || key.channel().keyFor(newSelector) != null) {
                            continue;
                        }
                        int interestOps = key.interestOps();
                        key.cancel();
                        key.channel().register(newSelector, interestOps, a);
                        nChannels++;
                    } catch (Exception e) {
                        LOGGER.warn("Failed to re-register a Channel to the new Selector.", e);
                    }
                }
            } catch (ConcurrentModificationException e) {
                // Probably due to concurrent modification of the key set.
                continue;
            }
            break;
        }
        oldSelector.close();
        return newSelector;
    }
}
