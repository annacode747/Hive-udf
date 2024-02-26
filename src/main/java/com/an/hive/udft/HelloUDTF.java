package com.an.hive.udft;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;
/**
 * 数据：{"movie": [{"movie_name": "肖申克的救赎", "movie_type": "犯罪" }, {"movie_name": "肖申克的救赎",
 "movie_type": "剧情" }]}
 * 需求：从一行 JSON 格式数据中取出 movie_name 和 movie_type 两个 Key 及其对应的 Value。K-V 输出的格式为：
 * movie_name   movie_type
 * 肖申克的救赎   犯罪
 * 肖申克的救赎   剧情
 */
public class HelloUDTF extends GenericUDTF {
    /**
     * 实例化 UDTF 对象，判断传入参数的长度以及数据类型
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {
        // 获取入参
        List<? extends StructField> fieldRefs = argOIs.getAllStructFieldRefs();
        // 参数校验，判断传入参数的长度以及数据类型
        if (fieldRefs.size() != 1) {
            throw new UDFArgumentLengthException("参数个数必须为 1");
        }
        if
        (!ObjectInspector.Category.PRIMITIVE.equals(fieldRefs.get(0).getFieldObjectInspector().getCategory()))
        {
            /*
            UDFArgumentTypeException(int argumentId, String message)
                异常对象需要传入两个参数：
                    int argumentId：参数的位置，ObjectInspector 中的下标
                    String message：异常提示信息
             */
            throw new UDFArgumentTypeException(0, "参数类型必须为 String");
        }
        // 创建输出字段名称的数组以及字段数据类型的数组
        ArrayList<String> columNames = new ArrayList<>();
        ArrayList<ObjectInspector> columType = new ArrayList<>();
        columNames.add("movie_name");
        columType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        columNames.add("movie_type");
        columType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // 自定义函数输出的数据类型
        return ObjectInspectorFactory.getStandardStructObjectInspector(columNames, columType);
    }
    /**
     * 处理数据
     *
     * @param args
     * @throws HiveException
     * */
    @Override
    public void process(Object[] args) throws HiveException {
        String[] outline = new String[2];
        if (args[0] != null) {
            JSONObject jsonObject = new JSONObject(args[0].toString());
            JSONArray jsonArray = jsonObject.getJSONArray("movie");
            for (int i = 0; i < jsonArray.length(); i++) {
                outline[0] = jsonArray.getJSONObject(i).getString("movie_name");
                outline[1] = jsonArray.getJSONObject(i).getString("movie_type");
                // 将处理好的数据通过 forward 方法将数据按行写出
                forward(outline);
            }
        } else {
            outline[0] = null;
            outline[1] = null;
            forward(outline);
        }
    }
    @Override
    public void close() throws HiveException {
    }
}

