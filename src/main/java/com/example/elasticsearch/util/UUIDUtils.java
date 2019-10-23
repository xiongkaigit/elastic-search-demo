/**
 * Author:   xiongkai
 * Date:     2019-07-26 11:33
 */
package com.example.elasticsearch.util;

import java.util.UUID;

public class UUIDUtils {

    public static String getUUID(){
        return UUID.randomUUID().toString().replace("-", "");
    }

}
