import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class Sparkjava2
{
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparktraining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rd1 = sc.parallelize(Arrays.asList(1,2,3, 4, 5, 6, 7, 8));
        JavaRDD<Integer> rd2=sc.parallelize(Arrays.asList(3,4,5,6));
        Integer sum=rd1.reduce(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i, Integer j) throws Exception {
              return i*j;
          }
      });

        System.out.println("Sum : " + sum);

        System.out.println("Number of elements in RDD1: " +rd1.count());

        SimpleDateFormat sdf = new SimpleDateFormat("dd:mm:yy:hh:mm:ss");

        System.out.println(Calendar.getInstance().getTimeInMillis());


        rd1.persist(StorageLevel.MEMORY_ONLY());

        System.out.println("First Element : " + rd1.first());

        System.out.println("First Three element : " + rd1.take(3) );

        System.out.println("Output of top : " + rd1.top(4));

        System.out.println("Count by Value : " + rd1.countByValue());

        System.out.println("takeOrder : " +rd1.takeOrdered(5));

        System.out.println("takeSample with Replacement : " + rd1.takeSample(true,6,1));

        System.out.println("takeSample without Replacement :" + rd1.takeSample(false,6,1));

        System.out.println(Calendar.getInstance().getTimeInMillis());
        rd1.unpersist();




        System.out.println("Union of Rdd:");

        JavaRDD<Integer> rd3 = rd1.union(rd2);
        System.out.println(rd3.collect());

        System.out.println("Intersection of Rdd:");
        System.out.println(rd1.intersection(rd2).collect());
        System.out.println("SubStraction:");

        System.out.println(rd1.subtract(rd2).collect());

        System.out.println(rd1.cartesian(rd2).collect());

        System.out.println("Sample:");

        System.out.println(rd1.sample(true, 0.25, 1).collect());
        System.out.println(rd1.sample(false, 0.75, 1).collect());
    }
}
