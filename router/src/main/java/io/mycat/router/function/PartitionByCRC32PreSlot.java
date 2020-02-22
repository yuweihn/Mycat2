package io.mycat.router.function;

import io.mycat.router.hashFunction.PureJavaCrc32HashFunction;

/**
 * 自动迁移御用分片算法，预分slot 102400个，映射到dn上，再conf下会保存映射文件，请不要修改
 *
 * @author nange magicdoom@gmail.com
 * @author chenjunwen 294712221@qq.com
 */
public class PartitionByCRC32PreSlot extends ConsistentHashPreSlot {
    public PartitionByCRC32PreSlot() {
        super("PartitionByCRC32PreSlot", 102400, new PureJavaCrc32HashFunction());
    }
}