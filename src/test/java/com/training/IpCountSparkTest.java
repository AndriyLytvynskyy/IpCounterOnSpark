package com.training;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class IpCountSparkTest implements Serializable {

    final String testInput1 = "ip1057 - - [26/Apr/2011:14:54:08 -0400] \"GET /faq/scascsi.jpg HTTP/1.1\" 200 35727 \"http://host2/faq/\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-us) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5\";";
    final String testInput3 = "ip1057 - - [26/Apr/2011:14:54:08 -0400] \"GET /faq/scascsi.jpg HTTP/1.1\" 200 2000 \"http://host2/faq/\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-us) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5\";";
    final String testInput2 = "ip1058 - - [26/Apr/2011:14:54:08 -0400] \"GET /faq/scascsi.jpg HTTP/1.1\" 200 35727 \"http://host2/faq/\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-us) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5\";";

    @Before
    public void setUp(){
         System.setProperty("hadoop.home.dir", "c:\\Tools\\hadoop");
    }

    @Test
    public void testIpCountSpark(){

        SparkConf sparkConf = new SparkConf().setAppName("IpCountSparkTest");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = new ArrayList<String>();
        list.add(testInput1);
        list.add(testInput2);
        list.add(testInput3);

        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list);
        JavaPairRDD<String, IpCountSpark.IpCountStatistics> pairs =  IpCountSpark.getIpCountStatisticPairs(stringJavaRDD);

        List<Tuple2<String, IpCountSpark.IpCountStatistics>> expectedTupleList = new ArrayList<>();
        Tuple2<String, IpCountSpark.IpCountStatistics> tuple1 = new Tuple2<>("ip1057", new IpCountSpark.IpCountStatistics(37727,2));
        Tuple2<String, IpCountSpark.IpCountStatistics> tuple2 = new Tuple2<>("ip1058", new IpCountSpark.IpCountStatistics(35727,1));
        expectedTupleList.add(tuple1);
        expectedTupleList.add(tuple2);


        List<Tuple2<String, IpCountSpark.IpCountStatistics>> output = pairs.collect();
        for (Tuple2<String, IpCountSpark.IpCountStatistics> expectedTuple : expectedTupleList) {
            assert output.contains(expectedTuple);
        }

        assert "ip1057".equals(IpCountSpark.getIPWithMaxBytes(pairs));

    }
}
