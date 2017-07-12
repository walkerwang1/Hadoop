package com.walkerwang.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

/*
 * 1-对用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
 * 2-对物品组合列表进行计数，建立物品的同现矩阵
 * 3-合并同现矩阵和评分矩阵	 
 * 4-计算推荐结果列表
 */
public class Recommend {

	public static final String HDFS = "hdfs://192.168.50.134:9000";
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static void main(String[] args) throws Exception {
		Map<String, String> path = new HashMap<>();
		path.put("data", "d:/Hadoop/file/recommend/small.csv");
		
		path.put("Step1Input", HDFS + "/user/hdfs/recommend");
		path.put("Step1Output", path.get("Step1Input") + "/step1");
		
		path.put("Step2Input", path.get("Step1Output"));
		path.put("Step2Output", path.get("Step1Input") + "/step2");
		
		path.put("Step3Input1", path.get("Step1Output"));
		path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
		path.put("Step3Input2", path.get("Step2Output"));
		path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
		
		path.put("Step4Input1", path.get("Step3Output1"));
		path.put("Step4Input2", path.get("Step3Output2"));
		path.put("Step4Output", path.get("Step1Input") + "/step4");

		Step1.run(path);
		Step2.run(path);
		Step3.run1(path);
		Step3.run2(path);
		Step4.run(path);
		System.exit(0);
	}

	public static JobConf config() {
		JobConf conf = new JobConf(Recommend.class);
		conf.setJobName("Recommend");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
}
