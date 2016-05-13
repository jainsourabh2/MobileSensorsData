package com.spark.sprak_kafka;

import org.apache.spark.api.java.function.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import scala.Tuple3;

public class RawJSONParser implements Function<String, Tuple3<Long, String, Double>> {
	private final ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public Tuple3<Long,String,Double> call(String message)
	{
		try{
			JsonNode root = mapper.readValue(message, JsonNode.class);
			Long id =1L;
			String values;
			Double xval,yval,zval,sum=100.01;
			String text;
			//final JsonNode sensor = root.get("sensor");
			text = root.get("sensor").textValue();
			values = root.get("values").toString();
			JsonNode vals = mapper.readValue(values, JsonNode.class);
			
			if(text.equals("compass")){
				xval = vals.get("x").asDouble();
				yval = vals.get("y").asDouble();
				zval = vals.get("z").asDouble();
				//sum = xval + yval + zval;
				sum = yval;
			} else if(text.equals("accel")){
				xval = vals.get("x").asDouble();
				yval = vals.get("y").asDouble();
				zval = vals.get("z").asDouble();
				//sum = xval + yval + zval;
				sum = xval;
			} else if (text.equals("light")){
				sum = vals.get("value").asDouble();
			} else if (text.equals("proximity")){
				sum = vals.get("value").asDouble();
			}			
			
			
			//System.out.println(text);
			return new Tuple3<Long, String, Double>(id, text, sum);
		}
		catch (Exception ex){
			System.out.println(ex);
		}
		return null;
	}

}
