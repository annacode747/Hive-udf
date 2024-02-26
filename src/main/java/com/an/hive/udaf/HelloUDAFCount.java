package com.an.hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import java.util.HashSet;
/**
 * 需求：实现自定义统计函数
 */
public class HelloUDAFCount extends AbstractGenericUDAFResolver {
/**
 * 检查参数长度和类型
 * 根据参数返回对应的实际处理对象
 *
 * @param parameters
 * @return
 * @throws SemanticException
 */
@Override
public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
        throws SemanticException {
            return new HelloUDAFCountEvaluator();
    }
    public static class HelloUDAFCountEvaluator extends GenericUDAFEvaluator {
        /**
         * 存放最终结果
         */
        private LongWritable result;
        /**
         * 用于计算
         */
        private long value;
        /**
         * 检查器对象
         */
        private LongObjectInspector partialCountAggOI;
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException {
            System.out.println("HelloUDAFSumEvaluator.init[负责初始化计算函数并设置它的内部状态]");
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL2 || mode == Mode.FINAL) {
                partialCountAggOI = (LongObjectInspector) parameters[0];
            }
            result = new LongWritable(0);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }
        /**
         * 用于存储计数值的类
         */
        @AggregationType(estimable = true)
        static class CountAgg extends AbstractAggregationBuffer {
            HashSet<ObjectInspectorUtils.ObjectInspectorObject> uniqueObjects;
            long value;
        }
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            System.out.println("HelloUDAFCountEvaluator.getNewAggregationBuffer");
            CountAgg buffer = new CountAgg();
            reset(buffer);
            return buffer;
        }
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            System.out.println("HelloUDAFCountEvaluator.reset");
            ((CountAgg) agg).value = 0;
            ((CountAgg) agg).uniqueObjects = new HashSet<ObjectInspectorUtils.ObjectInspectorObject>
                    ();
        }
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            System.out.println("HelloUDAFSumEvaluator.iterate[每次对一个新值进行聚集计算都会调用 iterate 方法]");
            if (parameters == null) {
                return;
            }
            ((CountAgg) agg).value++;
        }
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            System.out.println("HelloUDAFSumEvaluator.terminatePartial[Hive 需要部分聚集结果的时候会调用 该方法]");
            return terminate(agg);
        }
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            System.out.println("HelloUDAFSumEvaluator.merge[合并两个部分聚集值会调用这个方法]");
            if (partial != null) {
                CountAgg countAgg = (CountAgg) agg;
                long p = partialCountAggOI.get(partial);
                countAgg.value += p;
            }
        }
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            System.out.println("HelloUDAFSumEvaluator.terminate[Reduce 最终返回的结果]");
            result.set(((CountAgg) agg).value);
            return result;
        }
    }
}

