package com.an.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashMap;


@Description(name = "collect",
        value = "_FUNC_(x) - Returns an array of all the elements in the aggregation group "
)
public class CollectUDAF extends AbstractGenericUDAFResolver {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // TODO Auto-generated method stub
        if (parameters.length != 1 && parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "One argument is expected to return an Array, Two arguments are expected for a Map.");
        }
        if (parameters.length == 1) {
            return new ArrayCollectUDAFEvaluator();
        } else {
            return new MapCollectUDAFEvaluator();
        }
    }

    public static class ArrayCollectUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private ObjectInspector inputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector loi;
        private StandardListObjectInspector internalMergeOI;


        static class ArrayAggBuffer implements AggregationBuffer {
            ArrayList collectArray = new ArrayList();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1) {
                inputOI = parameters[0];
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(ObjectInspectorUtils
                                .getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    //no map aggregation.
                    inputOI = ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory
                            .getStandardListObjectInspector(inputOI);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new ArrayAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object p = parameters[0];

            if (p != null) {
                ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
                putIntoSet(p, myagg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
            for (Object i : partialResult) {
                putIntoSet(i, myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            ArrayAggBuffer arrayBuff = (ArrayAggBuffer) buff;
            arrayBuff.collectArray = new ArrayList();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.collectArray.size());
            ret.addAll(myagg.collectArray);
            return ret;

        }

        private void putIntoSet(Object p, ArrayAggBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
                    this.inputOI);
            myagg.collectArray.add(pCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.collectArray.size());
            ret.addAll(myagg.collectArray);
            return ret;
        }
    }

    public static class MapCollectUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputKeyOI;
        private ObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector moi;
        private StandardMapObjectInspector internalMergeOI;


        static class MapAggBuffer implements AggregationBuffer {
            HashMap<Object, Object> collectMap = new HashMap<Object, Object>();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                inputValOI = parameters[1];

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
                        ObjectInspectorUtils.getStandardObjectInspector(inputValOI));
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    inputKeyOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    inputValOI = ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[1]);
                    return (StandardMapObjectInspector) ObjectInspectorFactory
                            .getStandardMapObjectInspector(inputKeyOI, inputValOI);
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = internalMergeOI.getMapValueObjectInspector();
                    moi = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return moi;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object k = parameters[0];
            Object v = parameters[1];

            if (k != null) {
                MapAggBuffer myagg = (MapAggBuffer) agg;
                putIntoSet(k, v, myagg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Object i : partialResult.keySet()) {
                putIntoSet(i, partialResult.get(i), myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.collectMap = new HashMap<Object, Object>();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;

        }

        private void putIntoSet(Object key, Object val, MapAggBuffer myagg) {
            Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI);
            Object valCopy = ObjectInspectorUtils.copyToStandardObject(val, this.inputValOI);

            myagg.collectMap.put(keyCopy, valCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;
        }
    }


}