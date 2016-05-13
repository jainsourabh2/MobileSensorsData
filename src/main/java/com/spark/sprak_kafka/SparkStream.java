package com.spark.sprak_kafka;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.streaming.Duration;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import java.util.Map;
import java.util.HashMap;

public class SparkStream {
	//private static final Pattern SPACE = Pattern.compile(" ");
	private static int DURATION_MILLISECS = 2000;
    public static void main(String args[])
    {
        if(args.length != 3)
        {
            System.out.println("SparkStream <zookeeper_ip> <group_nm> <topic1,topic2,...>");
            System.exit(1);
        }

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Map<String,Integer> topicMap = new HashMap<String,Integer>();
        String[] topic = args[2].split(",");
        for(String t: topic)
        {
            topicMap.put(t, new Integer(3));
        }

        JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "SparkStream", new Duration(DURATION_MILLISECS));
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap );

        System.out.println("Connection done++++++++++++++");
        
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() 
                                                {
                                                    /**
													 * 
													 */
													private static final long serialVersionUID = 1L;

													public String call(Tuple2<String, String> message)
                                                    {
														//System.out.println(message);	
                                                    	return message._2();
                                                    }
                                                }
                                                );
        
        /*Parsing JSON*/
        JavaDStream<Tuple3<Long, String, Double>> pmess = lines.map(new RawJSONParser());
        
        /*Removing NULL messages*/
        JavaDStream<Tuple3<Long, String, Double>> filteredpmess = pmess.filter(
                new Function<Tuple3<Long, String, Double>, Boolean>() {
                  /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				@Override
                  public Boolean call(Tuple3<Long, String, Double> sensor) throws Exception {
                    return sensor != null;
                  }
                });

        /*Filtering Compass Messages*/
        JavaDStream<Tuple3<Long, String, Double>> compassfilteredpmess = filteredpmess.filter(
                new Function<Tuple3<Long, String, Double>, Boolean>() {
                  /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				@Override
                  public Boolean call(Tuple3<Long, String, Double> sensor) throws Exception {
					if(sensor._2().equals("compass")){
						return sensor != null;
					}
					return false;
                  }
                });        
        
        /*Converting it to a KeyValue Pair D Stream*/
        
        //Data Type for compasspairs object//
        JavaPairDStream<String, Double> compasspairs = compassfilteredpmess.mapToPair(
        		//Input and Output Data Types//
        		new PairFunction<Tuple3<Long, String, Double>, String, Double>() {
        			/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override 
        			//Return Type and Input Type
        			public Tuple2<String, Double> call(Tuple3<Long, String, Double> message) throws Exception{
        				//Return Type
        				return new Tuple2<String,Double>(message._2(),message._3());
        			}
				}
        		);
        
        JavaPairDStream<String,Double> compasswordsSummation = compasspairs.reduceByKey(
        		new Function2<Double, Double, Double>() {					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double v1, Double v2) throws Exception {
						return v1 + v2;
					}
				});
        
        
        JavaDStream<Double> dummy = compasswordsSummation.map(
        		new Function<Tuple2<String,Double>,Double>(){
        			/**
					 * 
					 */
					private static final long serialVersionUID = 6078061845181884129L;

					@Override
        			public Double call (Tuple2<String,Double> msg) throws Exception{
						if((msg._2()/Double.parseDouble("400"))<Double.parseDouble("-3")){
							System.out.println("Fallen Down");
						}
        				return msg._2();
        			}
        		});
        
        compassfilteredpmess.count().print();
        dummy.print();
        
        /*
        if(Double.parseDouble(dummy.toString()) > Double.parseDouble("100"))
        {
            dummy.print();
        }
        if(Long.parseLong(compassfilteredpmess.count().toString()) > Long.parseLong("10"))
        {
        	compassfilteredpmess.count().print();
        }
        */
        
        /*
        compasswordsSummation.foreachRDD(new Function<JavaPairRDD<Tuple2<String,Double>>, Void>(){
        	@Override
        	public Void call(JavaPairRDD<Tuple2<String, Double>> msg) throws Exception{
        		if(msg.)
        		{
        	}
        });
        */

        
        //JavaPairDStream<Long> compassCount = compasspairs.count();
        //System.out.println(compassCount);
        
        //lines.print();
        /*
        JavaDStream<String> compassfilter = lines.filter(new Function <String,Boolean>(){

			@Override
        	public Boolean call(String s) throws Exception{
        		if(s.contains("compass") && s.length()>0){
        			return true;
        		}
        		return false;
        	}
        });
                     
        JavaDStream<String> accessfilter = lines.filter(new Function <String,Boolean>(){

			@Override
        	public Boolean call(String s) throws Exception{
        		if(s.contains("accel") && s.length()>0){
        			return true;
        		}
        		return false;
        	}
        });
        
        JavaDStream<String> lightfilter = lines.filter(new Function <String,Boolean>(){

			@Override
        	public Boolean call(String s) throws Exception{
        		if(s.contains("light") && s.length()>0){
        			return true;
        		}
        		return false;
        	}
        });
              
        compassfilter.count().print();
        accessfilter.count().print();
        lightfilter.count().print();
        */
        
        //List<String> jsonData = Arrays.asList(compassfilter);
        
        /*
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x){
				return Lists.newArrayList(SPACE.split(x));
			}
		});
        
		words.print();
		words.count().print();
		*/
        jssc.start();
        jssc.awaitTermination();

    }
}