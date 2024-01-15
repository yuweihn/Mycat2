/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat;

import io.mycat.beans.mysql.packet.MySQLPayloadReadView;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * @author mycat
 */
public class BindValueUtil {

    public static final void read(MySQLPayloadReadView mm, BindValue bv, Charset charset, boolean hasBlob) {
        switch (bv.type & 0xff) {
            case MysqlDefs.FIELD_TYPE_BIT:
                bv.value = mm.readLenencBytes();
                break;
            case MysqlDefs.FIELD_TYPE_TINY:
                bv.byteBinding = mm.readByte();
                break;
            case MysqlDefs.FIELD_TYPE_SHORT:
                bv.shortBinding = (short) mm.readFixInt(2);
                break;
            case MysqlDefs.FIELD_TYPE_LONG:
                bv.intBinding = (int) mm.readFixInt(4);
                break;
            case MysqlDefs.FIELD_TYPE_LONGLONG:
                bv.longBinding = mm.readFixInt(8);
                break;
            case MysqlDefs.FIELD_TYPE_FLOAT:
                bv.floatBinding = mm.readFloat();
                break;
            case MysqlDefs.FIELD_TYPE_DOUBLE:
                bv.doubleBinding = mm.readDouble();
                break;
            case MysqlDefs.FIELD_TYPE_TIME:
                bv.value = mm.readTime();
                break;
            case MysqlDefs.FIELD_TYPE_DATE:
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                bv.value = mm.readDate();
                break;
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_BLOB:
            default: {
                if (hasBlob) {
                    byte[] vv = mm.readLenencBytes();
                    if (vv == null) {
                        bv.isNull = true;
                    } else {
                        if (charset == null) {
                            charset = StandardCharsets.UTF_8;
                        }
                        try {
                            bv.value = charset.newDecoder().onMalformedInput(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(vv)).toString();
                        } catch (CharacterCodingException e) {
                            bv.value = vv;
                        }
                    }
                } else {
                    String vv = mm.readLenencString();
                    if (vv == null) {
                        bv.isNull = true;
                    } else {
                        bv.value = vv;
                    }
                }
            }
        }
        bv.isSet = true;
    }

}