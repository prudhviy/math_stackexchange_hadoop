package math_stack;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class Query2 extends Configured implements Tool {

   public static class QueryMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String json_line = value.toString();
         List<String> tags_list = new ArrayList<String>();
         Configuration conf = context.getConfiguration();
         String year_param = conf.get("year");
         
         try {
            JSONParser parser = new JSONParser();
            JSONObject json_obj;
            json_obj = (JSONObject) parser.parse(json_line);
            String doc_type = (String) json_obj.get("type");
            
            if(doc_type.equals("posts")) {
               String tags_str = (String) json_obj.get("Tags");
               DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
               String creation_date_text = (String) json_obj.get("CreationDate");
               Date creation_date = df2.parse(creation_date_text);
               int month = creation_date.getMonth() + 1;
               int tags_count = StringUtils.countMatches(tags_str, "<");
               int year = creation_date.getYear() + 1900;
               
               if(tags_count == 1) {
                  tags_str = tags_str.replaceAll("<", "");
                  tags_str = tags_str.replaceAll(">", "");
                  tags_list.add(tags_str);
               } else if(tags_count > 1) {
                  tags_str = tags_str.replaceAll("><", " ");
                  tags_str = tags_str.replaceAll("<", "");
                  tags_str = tags_str.replaceAll(">", "");
                  String[] parts = tags_str.split(" ");
                  for(int i = 0; i < parts.length; i++) {
                     tags_list.add(parts[i]);
                  }
               }
               
               if(tags_count > 0 && year == Integer.parseInt(year_param)) {
                  for(int i = 0; i < tags_list.size(); i++) {
                     context.write(new Text(tags_list.get(i) + "_" + String.valueOf(month)), new IntWritable(1));
                  }
               }
            }
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }
 
   public static class QueryReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
         int totalVal = 0;
         for (IntWritable value : values) {
            totalVal += value.get();
         }
         context.write(key, new IntWritable(totalVal));
      }
   }
   
   public static class QueryMapper2 extends Mapper<LongWritable, Text, StockKey, Text>{
      
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String line = value.toString();
         
         try {
            String[] parts = line.split("\t");
            String comp_key = parts[0];
            String occurence = parts[1];
            String[] parts1 = comp_key.split("_");
            String tag_name = parts1[0];
            String month = parts1[1];
            StockKey stockKey = new StockKey(tag_name, Integer.parseInt(month));
            context.write(stockKey, new Text(occurence));
               
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }
   
   public static void main(String[] args) throws Exception {
      if(args.length != 3) {
         System.err.println("Usage : Query2 <inputPath> <output Path>");
         System.exit(-1);
      }
      
      ToolRunner.run(new Configuration(), new Query2(), args);   
   }
   
   private String OUTPUT_PATH = "intermediate_output";
   
   @SuppressWarnings("deprecation")
   public int run(String[] args) throws Exception {
      Random rand = new Random();
      int randomNum = rand.nextInt((1000 - 1) + 1) + 1;
      OUTPUT_PATH = OUTPUT_PATH + String.valueOf(randomNum);
      
      Configuration conf = getConf();
      conf.set("year", args[2]);
      conf.setInt(MRJobConfig.NUM_MAPS, 10);
      conf.setInt(MRJobConfig.NUM_REDUCES, 4);
      
      Job job = new Job(conf, "Query2");
      
      job.setJarByClass(Query2.class);
      job.setJobName("Math Stack Query2");
      
      job.setMapperClass(QueryMapper.class);
      job.setCombinerClass(QueryReducer.class);
      job.setReducerClass(QueryReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      TextInputFormat.addInputPath(job, new Path(args[0]));
      TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
      
      job.waitForCompletion(true);
      
      // =======================================
      
      Configuration conf2 = getConf();
      conf2.setInt(MRJobConfig.NUM_MAPS, 10);
      conf2.setInt(MRJobConfig.NUM_REDUCES, 4);
      
      Job job2 = new Job(conf2, "Query2-2");
      job2.setJarByClass(Query2.class);

      job2.setMapperClass(QueryMapper2.class);
      
      job2.setMapOutputKeyClass(StockKey.class);
      job2.setMapOutputValueClass(Text.class);

      job2.setOutputKeyClass(StockKey.class);
      job2.setOutputValueClass(Text.class);
      
      job2.setPartitionerClass(KeyPartitioner.class);
      job2.setGroupingComparatorClass(KeyGroupingComparator.class);
      job2.setSortComparatorClass(CompositeKeyComparator.class);
      
      TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
      TextOutputFormat.setOutputPath(job2, new Path(args[1]));
      
      return job2.waitForCompletion(true) ? 0 : 1;

   }
}
