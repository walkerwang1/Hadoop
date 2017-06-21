package com.walkerwang.pagerank;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class PageRankJob {

	public static final String HDFS = "hdfs://192.168.50.134:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
         Map<String, String> path = pagerank(); //pagerank数据集

//        Map<String, String> path = peoplerank();// peoplerank数据集

        //多个Job，工作流应用程序：AdjacencyMatrix-->PageRank-->Normal
        try {
        	//初始邻接矩阵构建
            AdjacencyMatrix.run(path);
            int iter = 5;
            //PageRank的迭代过程
            for (int i = 0; i < iter; i++) {// 迭代执行
            	System.out.println("第"+ (i+1) + "次迭代");
                PageRank.run(path);
            }
            //规范化处理
            Normal.run(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private static Map<String, String> peoplerank() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("page", "d:/Hadoop/file/peoplepank/people.csv");// 本地的数据文件
        path.put("pr", "d:/Hadoop/file/peoplepank/peoplerank.csv");// 本地的数据文件
        path.put("nums", "25");// 用户数
        path.put("d", "0.85");// 阻尼系数

        path.put("input", HDFS + "/user/hdfs/pagerank");// HDFS的目录
        path.put("input_pr", HDFS + "/user/hdfs/pagerank/pr");// pr存储目
        path.put("tmp1", HDFS + "/user/hdfs/pagerank/tmp1");// 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/user/hdfs/pagerank/tmp2");// 临时目录,计算到得PR,覆盖input_pr

        path.put("result", HDFS + "/user/hdfs/pagerank/result");// 计算结果的PR
        return path;

    }
    
    private static Map<String, String> pagerank() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("page", "d:/Hadoop/file/pagerank/page.csv");// 本地的数据文件
        path.put("pr", "d:/Hadoop/file/pagerank/pr.csv");// 本地的数据文件
        path.put("nums", "4");// 页面数
        path.put("d", "0.85");// 阻尼系数

        path.put("input", HDFS + "/user/hdfs/pagerank");// HDFS的目录
        path.put("input_pr", HDFS + "/user/hdfs/pagerank/pr");// pr存储目录
        path.put("tmp1", HDFS + "/user/hdfs/pagerank/tmp1");// 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/user/hdfs/pagerank/tmp2");// 临时目录,计算到得PR,覆盖input_pr

        path.put("result", HDFS + "/user/hdfs/pagerank/result");// 计算结果的PR
        return path;
    }
    
    /*
     * Hadoop集群的远程配置信息
     */
    public static JobConf config() {	
    	JobConf conf = new JobConf(PageRankJob.class);
        conf.setJobName("PageRank");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    
    /*
     * 保留6位小数
     */
    public static String scaleFloat(float f) {
    	DecimalFormat df = new DecimalFormat("##0.000000");
    	return df.format(f);
    }
}
