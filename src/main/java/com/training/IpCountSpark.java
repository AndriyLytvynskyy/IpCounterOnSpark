package com.training;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 Driver class
*/
public class IpCountSpark implements Serializable {

    private static final Pattern pattern = Pattern.compile("(ip\\d+)\\s.*\"GET.*?\"\\s\\d+\\s(\\d+)(.*)");

    public final static class IpCountStatistics implements Serializable {
        private int totalNumberOfBytes = 0;

        private int numberOfRequestsPerIp = 0;

        public IpCountStatistics(int totalNumberOfBytes, int numberOfRequestsPerIp) {
            this.totalNumberOfBytes = totalNumberOfBytes;
            this.numberOfRequestsPerIp = numberOfRequestsPerIp;
        }

        public int getTotalNumberOfBytes() {
            return totalNumberOfBytes;
        }

        public void setTotalNumberOfBytes(int totalNumberOfBytes) {
            this.totalNumberOfBytes = totalNumberOfBytes;
        }

        public IpCountStatistics merge(IpCountStatistics other) {
            return new IpCountStatistics(totalNumberOfBytes + other.totalNumberOfBytes, numberOfRequestsPerIp + other.numberOfRequestsPerIp);
        }

        public String toString() {
            return String.format("%s\t%s", ((numberOfRequestsPerIp != 0)?((float)totalNumberOfBytes / numberOfRequestsPerIp):0), totalNumberOfBytes);
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IpCountStatistics)) return false;

            IpCountStatistics that = (IpCountStatistics) o;

            if (numberOfRequestsPerIp != that.numberOfRequestsPerIp) return false;
            if (totalNumberOfBytes != that.totalNumberOfBytes) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = totalNumberOfBytes;
            result = 31 * result + numberOfRequestsPerIp;
            return result;
        }
    }


    public static String extractIp(String line) {
        Matcher m = pattern.matcher(line);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    public static IpCountStatistics extractIpCountStatistics(String line) {
        Matcher m = pattern.matcher(line);
        if (m.find()) {
            int totalNumberOfBytes = Utils.toInteger(m.group(2));
            return new IpCountStatistics(totalNumberOfBytes, 1);
        }
        return new IpCountStatistics(0, 0);
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("IpCountWithSpark");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile("/user/root/dir1/input/", 1);

        JavaPairRDD<String, IpCountStatistics> counts = getIpCountStatisticPairs(lines);

        List<Tuple2<String, IpCountStatistics>> output = counts.collect();
        for (Tuple2<?, ?> t : output) {
            System.out.println(t._1() + "\t" + t._2());
        }
        System.out.println("IP with max bytes = " + getIPWithMaxBytes(counts));

        ctx.stop();
    }

    public static String getIPWithMaxBytes(JavaPairRDD<String, IpCountSpark.IpCountStatistics> output) {
        List<Tuple2<String, IpCountSpark.IpCountStatistics>> top = output.top(1, new ValueComparator());
        return top.get(0)._1();
    }

    public static JavaPairRDD<String, IpCountStatistics> getIpCountStatisticPairs(JavaRDD<String> lines) {
        JavaPairRDD<String, IpCountStatistics> extracted = lines.mapToPair(new PairFunction<String, String, IpCountStatistics>() {
            @Override
            public Tuple2<String, IpCountStatistics> call(String s) {
                return new Tuple2<String, IpCountStatistics>(extractIp(s), extractIpCountStatistics(s));
            }
        });

        return extracted.reduceByKey(new Function2<IpCountStatistics, IpCountStatistics, IpCountStatistics>() {
            @Override
            public IpCountStatistics call(IpCountStatistics ipCount1, IpCountStatistics ipCount2) {
                return ipCount1.merge(ipCount2);
            }
        });
    }
}