package com.an.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
/**
 * 需求：在传入的字符串头部拼接 "Hello "
 */
public class HelloUDF extends GenericUDF {
    /**
     * 实例化 UDF 对象，判断传入参数的长度以及数据类型
     *
     * @param arguments 该参数为自定义函数的入参，是一个检查器对象的数组
     * @return 自定义函数输出的数据类型
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        // 一进一出，如果传入参数个数不为 1，就返回 UDF 长度异常
        // 根据需求修改代码，比如支持传入多个参数，那就可以循环处理
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("参数个数必须为 1");
        }
        // 获取参数，比对传入参数的数据类型
        /*
        ObjectInspector.Category 类型如下：
            PRIMITIVE：Hive 中 string、int、float、double、boolean 等基础数据类型
            LIST：数组类型
            MAP：Map类型
            STRUCT：结构体类型
         */
        // 第一个参数校验
        if (!ObjectInspector.Category.PRIMITIVE.equals(arguments[0].getCategory())) {
            /*
            UDFArgumentTypeException(int argumentId, String message)
                异常对象需要传入两个参数：
                    int argumentId：参数的位置，ObjectInspector 中的下标
                    String message：异常提示信息
             */
            throw new UDFArgumentTypeException(0, "参数类型必须为 String");
        }
        // 如果有更多参数需要校验就继续编码
        // 自定义函数输出的数据类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    /**
     * **** 处理数据
     *
     * @param arguments
     * @return
     * @throws HiveException
    }  */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 获取第 0 位参数的值，注意需要通过 get() 获取
        String arg1 = arguments[0].get().toString();
        // 如果有更多参数需要处理就继续编码
        return "Hello " + arg1;
    }
    /**
     * 获取 HQL 执行计划与流程
     * 例如：EXPLAIN SELECT AVG(sal), deptno FROM emp GROUP BY deptno;
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return "在字符前面加Hello";
    }
}

