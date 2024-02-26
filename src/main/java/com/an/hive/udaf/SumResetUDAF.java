package com.an.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
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

@Description(name = "SumReset_udaf",
        value = "_FUNC_(x) - sum>0 to sum is Reset\n UDAF function that accepts any type of input",
        extended = "Example:\n"
                + "  SELECT SumResetUDAF(column) over( partition by c1 order by c2) FROM table")
public class SumResetUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new SumResetUDAFEvaluator();
    }

    public static class SumResetUDAFEvaluator extends GenericUDAFEvaluator {
        private LongWritable result;
        private long value;
        private LongObjectInspector partialCountAggOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL2 || mode == Mode.FINAL) {
                if (parameters.length < 1) {
                    throw new HiveException("Partial or final mode requires at least one parameter");
                }
                partialCountAggOI = (LongObjectInspector) parameters[0];
            }
            result = new LongWritable(0);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        static class CountAgg extends AbstractAggregationBuffer {
            HashSet<ObjectInspectorUtils.ObjectInspectorObject> uniqueObjects;
            long value;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CountAgg buffer = new CountAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            CountAgg countAgg = (CountAgg) agg;
            countAgg.value = 0;
            countAgg.uniqueObjects = new HashSet<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters != null && parameters.length > 0 && parameters[0] != null) {
                CountAgg countAgg = (CountAgg) agg;
                if (countAgg.value<0){
                    countAgg.value += Integer.parseInt(parameters[0].toString());
                    return;
                }
                countAgg.value = Integer.parseInt(parameters[0].toString());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                CountAgg countAgg = (CountAgg) agg;
                long p = partialCountAggOI.get(partial);
                countAgg.value += p;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((CountAgg) agg).value);
            return result;
        }
    }
}
