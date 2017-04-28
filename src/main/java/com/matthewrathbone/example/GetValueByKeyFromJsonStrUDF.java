package com.matthewrathbone.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * 功能：给定一个json字符串，和json串的键值，返回value
 * Created by xuefeng on 2017/4/28.
 */
public class GetValueByKeyFromJsonStrUDF extends GenericUDF {
    StringObjectInspector first;
    StringObjectInspector second;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("GenericUDFDemo only takes 2 arguments: json String, json key");
        }
        // 1. 检查是否接收到正确的参数类型
        ObjectInspector jsonStr = arguments[0];
        ObjectInspector keyStr = arguments[1];
        if (!(jsonStr instanceof StringObjectInspector) || !(keyStr instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first and second argument must be a string");
        }
        first = (StringObjectInspector) jsonStr;
        second = (StringObjectInspector) keyStr;
        // 返回类型是boolean，所以我们提供了正确的object inspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        String arg1 = first.getPrimitiveJavaObject(arguments[0].get());
        String arg2 = second.getPrimitiveJavaObject(arguments[1].get());
        JSONObject json = JSON.parseObject(arg1);
        if (json != null) {
            if (json.containsKey(arg2))
                return json.getString(arg2);
        }
        return "";
    }

    @Override
    public String getDisplayString(String[] children) {
        return "get value from jsonStr by key";
    }
}
