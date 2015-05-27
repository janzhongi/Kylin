package org.apache.kylin.monitor;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by jiazhong on 2015/5/27.
 */
public class ParseApiRequestLogTest {


    @BeforeClass
    public static void beforeClass() throws Exception {
        //set catalina.home temp
        System.setProperty(ConfigUtils.CATALINA_HOME, "../server/");
    }


    @Test
    public void parseApiRequstLog(){

    }

}
