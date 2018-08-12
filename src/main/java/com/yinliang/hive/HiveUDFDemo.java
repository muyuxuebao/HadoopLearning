package com.yinliang.hive;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by muyux on 2016/1/12.
 */
public class HiveUDFDemo extends UDF {
    private static Map<Integer, String> genderMap = new HashMap<Integer, String>();

    static {
        genderMap.put(0, "female");
        genderMap.put(1, "male");
    }


    public Text evaluate(Text in) { // hive只会找以evaluate命名的函数。至于它的参数以及返回的类型，可以根据具体业务逻辑指定。
        String result = genderMap.get(Integer.parseInt(in.toString()));
        if (result == null) {
            result = "other";
        }
        return new Text(result);
    }
}
