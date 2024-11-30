package org.apache.bookkeeper.bookie.utils;

public enum ByteBufStatus {
    DEFAULT,
    ZERO_CAPACITY,
    FULL,
    NULL,
    ONLY_READ,
    LITTLE,
    CLOSE,
    MORE
}
