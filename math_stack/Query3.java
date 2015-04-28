package math_stack;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class Query3 extends Configured implements Tool {
   
   public static class QueryMapper extends Mapper<LongWritable, Text, Text, Text>{

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String json_line = value.toString();
         
         try {
            JSONParser parser = new JSONParser();
            JSONObject json_obj;
            json_obj = (JSONObject) parser.parse(json_line);
            String doc_type = (String) json_obj.get("type");
            
            if(doc_type.equals("posts")) {
               String post_type_id = (String) json_obj.get("PostTypeId");
               String tags = (String) json_obj.get("Tags");
               String date = (String) json_obj.get("CreationDate");
               
               
               if(post_type_id.equals("1") && tags.length() > 0) {
                  String parent_id = (String) json_obj.get("Id");
                  JSONObject postObject = new JSONObject();
                  postObject.put("tags", tags);
                  context.write(new Text(parent_id), new Text(postObject.toJSONString()));
               } else if(post_type_id.equals("2")) {
                  String parent_id = (String) json_obj.get("ParentId");
                  JSONObject answerObject = new JSONObject();
                  answerObject.put("count", 1);
                  answerObject.put("date", date);
                  context.write(new Text(parent_id), new Text(answerObject.toJSONString()));
               }
            }
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }
 
   public static class QueryReducer extends Reducer<Text, Text, Text, Text> {
      private static final Log _log = LogFactory.getLog(QueryReducer.class);
      
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         JSONArray arr = new JSONArray();
         JSONParser parser = new JSONParser();
         
         
         for (Text value: values) {
            try {
               JSONObject val;
               //_log.error("\n\n" + key + " " + value.toString() + "\n\n");
               val = (JSONObject) parser.parse(value.toString());
               //System.out.println("\n ################## \n");
               arr.add(val);
            } catch (Exception e) {
               // TODO Auto-generated catch block
               //_log.error("\n\n" + key + " " + value.toString() + "\n\n");
               e.printStackTrace();
            }
            
         }
         
         context.write(key, new Text(arr.toJSONString()));
      }
   }
   
   public static class QueryMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
      private static final Log _log = LogFactory.getLog(QueryMapper2.class);
      
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String line = value.toString();
         Configuration conf = context.getConfiguration();
         String year_param = conf.get("year");
         String temp1 = "";
         String temp = "";
         
         try {
            String[] parts = line.split("\t");
            JSONParser parser = new JSONParser();
            JSONObject json_obj;
            String input_key = parts[0];
            String arr_str = parts[1];
            List<String> tags_list = new ArrayList<String>();
            JSONArray json_arr;
            json_arr = (JSONArray) parser.parse(arr_str);
            Iterator itr = json_arr.iterator();
            int tags_count = 0;
            
            while (itr.hasNext()) {
               temp = itr.next().toString();
               json_obj = (JSONObject) parser.parse(temp);
               
               if(json_obj.containsKey("tags")) {
                  String tags_str = (String) json_obj.get("tags");
                  tags_count = StringUtils.countMatches(tags_str, "<");
                  
                  if(tags_count == 1) {
                     tags_str = tags_str.replaceAll("<", "");
                     tags_str = tags_str.replaceAll(">", "");
                     tags_list.add(tags_str);
                  } else if(tags_count > 1) {
                     tags_str = tags_str.replaceAll("><", " ");
                     tags_str = tags_str.replaceAll("<", "");
                     tags_str = tags_str.replaceAll(">", "");
                     String[] parts1 = tags_str.split(" ");
                     for(int i = 0; i < parts1.length; i++) {
                        tags_list.add(parts1[i]);
                     }
                  }
               }
            }
            
            Iterator itr2 = json_arr.iterator();
            
            while (itr2.hasNext()) {
               temp1 = itr2.next().toString();
               json_obj = (JSONObject) parser.parse(temp1);
               if(json_obj.containsKey("count")) {
                  String creation_date_text = (String) json_obj.get("date");
                  DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                  Date creation_date = df2.parse(creation_date_text);
                  int month = creation_date.getMonth() + 1;
                  int year = creation_date.getYear() + 1900;
                  
                  if(tags_count > 0 && year == Integer.parseInt(year_param)) {
                     for(int i = 0; i < tags_list.size(); i++) {
                        context.write(new Text(tags_list.get(i) + "_" + String.valueOf(month)), new IntWritable(1));
                     }
                  }
                  
               }
            }
         } catch (Exception e) {
            _log.error("\n\n HERE:" + temp1 + "\n\n");
            e.printStackTrace();
         }
      }  
   }
   
   public static class QueryReducer2 extends Reducer<Text, IntWritable, Text, IntWritable>{
      
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
         int totalVal = 0;
         for (IntWritable value : values) {
            totalVal += value.get();
         }
         context.write(key, new IntWritable(totalVal));
      }
   }
   
   public static void main(String[] args) throws Exception {
      if(args.length != 3) {
         System.err.println("Usage : Query3 <inputPath> <output Path>");
         System.exit(-1);
      }
      
      ToolRunner.run(new Configuration(), new Query3(), args);   
   }
   
   private String OUTPUT_PATH = "intermediate_output";
   
   @SuppressWarnings("deprecation")
   public int run(String[] args) throws Exception {
      String tmp = args[1];
      OUTPUT_PATH = OUTPUT_PATH + tmp.substring(1, tmp.length());
      
      Configuration conf = getConf();
      conf.setInt(MRJobConfig.NUM_MAPS, 10);
      conf.setInt(MRJobConfig.NUM_REDUCES, 4);
      
      Job job = new Job(conf, "Query3");
      
      job.setJarByClass(Query3.class);
      job.setJobName("Q3-J1 " + args[1]);
      
      job.setMapperClass(QueryMapper.class);
      //job.setCombinerClass(QueryReducer.class);
      job.setReducerClass(QueryReducer.class);
      
      //job.setMapOutputKeyClass(Text.class);
      //job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      
      TextInputFormat.addInputPath(job, new Path(args[0]));
      TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
      
      job.waitForCompletion(true);
      
      // =======================================
      
      Configuration conf2 = getConf();
      conf2.setInt(MRJobConfig.NUM_MAPS, 10);
      conf2.setInt(MRJobConfig.NUM_REDUCES, 4);
      conf2.set("year", args[2]);
      
      Job job2 = new Job(conf2, "Query3-2");
      job2.setJarByClass(Query3.class);
      
      job2.setJobName("Q3-J2 " + args[1]);
      job2.setMapperClass(QueryMapper2.class);
      //job2.setCombinerClass(QueryReducer2.class);
      job2.setReducerClass(QueryReducer2.class);
      
      //job2.setMapOutputKeyClass(Text.class);
      //job2.setMapOutputValueClass(IntWritable.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(IntWritable.class);
      
      //job2.setPartitionerClass(KeyPartitioner.class);
      //job2.setGroupingComparatorClass(KeyGroupingComparator.class);
      //job2.setSortComparatorClass(CompositeKeyComparator.class);
      
      TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
      TextOutputFormat.setOutputPath(job2, new Path(args[1]));
      
      job2.waitForCompletion(true);
      
      return 0;
   }
}
