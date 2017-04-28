package com.matthewrathbone.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

/**
 * Created by xuefeng on 2017/4/28.
 */
public class GetValueByKeyFromJsonStrUDFTest {
    JSONObject jsonObject = new JSONObject();

    {
        jsonObject.put("key", "1");
        jsonObject.put("entry", "2");
    }

    @Test
    public void genericUDRContainsKeyTest() throws HiveException {
        GetValueByKeyFromJsonStrUDF genericUDFDemo = new GetValueByKeyFromJsonStrUDF();
        String key = "key";
        genericUDFDemo.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector});
        Object ret = genericUDFDemo.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(jsonObject.toJSONString()), new GenericUDF.DeferredJavaObject(key)});
        System.out.println(ret);
        String key1 = "entry";
        Object ret1 = genericUDFDemo.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(jsonObject.toJSONString()), new GenericUDF.DeferredJavaObject(key1)});
        System.out.println(ret1);
    }

    @Test
    public void genericUDRNotContainsKeyTest() throws HiveException {
        GetValueByKeyFromJsonStrUDF genericUDFDemo = new GetValueByKeyFromJsonStrUDF();
        String key = "key2";
        genericUDFDemo.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector});
        Object ret = genericUDFDemo.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(jsonObject.toJSONString()), new GenericUDF.DeferredJavaObject(key)});
        System.out.println(String.format("value=%s", ret));
    }
}
