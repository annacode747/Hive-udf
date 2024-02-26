package com.an.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

@Description(name = "example_udaf",
        value = "_FUNC_(x) - Example UDAF function that accepts any type of input",
        extended = "Example:\n"
                + "  SELECT example_udaf(column) FROM table")
public class ExampleUDAF extends AbstractGenericUDAFResolver {

    public static class GenericUDAFEvaluatorAnyType extends GenericUDAFEvaluator {

        private IntWritable result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            result = new IntWritable(0);
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        public static class PartialResult {
            int sum;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            PartialResult result = new PartialResult();
            reset((AggregationBuffer) result);
            return (AggregationBuffer) result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((PartialResult) agg).sum = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null) {
                PartialResult result = (PartialResult) agg;
                result.sum += Integer.parseInt(parameters[0].toString());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((PartialResult) agg).sum;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                PartialResult result = (PartialResult) agg;
                result.sum += Integer.parseInt(partial.toString());
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((PartialResult) agg).sum;
        }
    }
}
