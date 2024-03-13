package com.an.hive.udaf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


@Description(name = "JsonMap",
        value = "_FUNC_(key, value) - Returns a JSON map with key-value pairs",
        extended = "Example:\n"
                + "  SELECT JsonMap(key, value) OVER (PARTITION BY c1 ORDER BY c2) FROM table")
public class JsonMap extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new JsonMapEvaluator();
    }

    public static class JsonMapEvaluator extends GenericUDAFEvaluator {
        private Map<Object, Object> result;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            result = new HashMap<>();
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        static class MapAgg extends AbstractAggregationBuffer {
            Map<Object, Object> map;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAgg buffer = new MapAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MapAgg mapAgg = (MapAgg) agg;
            mapAgg.map = new HashMap<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters != null && parameters.length == 2 && parameters[0] != null && parameters[1] != null) {
                MapAgg mapAgg = (MapAgg) agg;
                mapAgg.map.put(parameters[0], parameters[1]);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MapAgg mapAgg = (MapAgg) agg;
            return new HashMap<>(mapAgg.map);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                MapAgg mapAgg = (MapAgg) agg;
                if (partial instanceof LazyBinaryMap) {
                    LazyBinaryMap lazyBinaryMap = (LazyBinaryMap) partial;
                    Object realMap = lazyBinaryMap.getMap();
                    if (realMap instanceof Map) {
                        mapAgg.map.putAll((Map<?, ?>) realMap);
                    }
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAgg mapAgg = (MapAgg) agg;
            return new HashMap<>(mapAgg.map);
        }
    }
}
