package com.an.hive.buffer;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
public class FieldLengthAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    private Long value = 0L;

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public void add(Long addValue) {
        synchronized (value) {
            value += addValue;
        }
    }

    public void add(Long addValue,boolean is) {
        synchronized (value) {
            if (is)
                value = addValue;
        }
    }

    /**
     * 合并值缓冲区大小，这里是用来保存字符串长度，因此设为4byte
     * @return
     */
    @Override
    public int estimate() {
        return JavaDataModel.PRIMITIVES1;
    }
}
