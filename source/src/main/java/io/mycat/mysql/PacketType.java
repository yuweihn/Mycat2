package io.mycat.mysql;
/*
cjw
294712221@qq.com
 */
public enum PacketType {
    FULL(),
    LONG_HALF(),
    SHORT_HALF(),
    REST_CROSS(),
    FINISHED_CROSS(),
}