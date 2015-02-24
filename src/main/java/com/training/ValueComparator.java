package com.training;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class ValueComparator
        implements Comparator<Tuple2<String, IpCountSpark.IpCountStatistics>>, Serializable {

    @Override
    public int compare(Tuple2<String, IpCountSpark.IpCountStatistics> o1, Tuple2<String, IpCountSpark.IpCountStatistics> o2) {
        return o1._2().getTotalNumberOfBytes() < o2._2().getTotalNumberOfBytes() ? -1 : o1._2().getTotalNumberOfBytes() == o2._2().getTotalNumberOfBytes() ? 0 : 1;
    }


}
