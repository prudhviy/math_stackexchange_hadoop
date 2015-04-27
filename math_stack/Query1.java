package math_stack;


import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Query1 extends Configured implements Tool {
   public static class QueryMapper extends Mapper<LongWritable, Text, StockKey, IntWritable>{
      
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String json_line = value.toString();
         
         try {
            JSONParser parser = new JSONParser();
            JSONObject json_obj;
            json_obj = (JSONObject) parser.parse(json_line);
            String doc_type = (String) json_obj.get("type");
            
            if(doc_type.equals("votes")) {
               String vote_type_id = (String) json_obj.get("VoteTypeId");
               if(vote_type_id.equals("2")) {
                  StockKey stockKey = new StockKey(json_obj.get("PostId").toString(), randInt(1, 1000000));
                  context.write(stockKey, new IntWritable(1));
               }
            }
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
      
      public static int randInt(int min, int max) {

         // NOTE: Usually this should be a field rather than a method
         // variable so that it is not re-seeded every call.
         Random rand = new Random();

         // nextInt is normally exclusive of the top value,
         // so add 1 to make it inclusive
         int randomNum = rand.nextInt((max - min) + 1) + min;

         return randomNum;
     }
   }
 
   public static class QueryReducer extends Reducer<StockKey, IntWritable, StockKey, IntWritable>{
	 
      @Override
      public void reduce(StockKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
         int totalVal = 0;
         for (IntWritable value : values) {
            totalVal += value.get();
         }
         context.write(key, new IntWritable(totalVal));
      }
   }
   
   public static class QueryMapper2 extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
      
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String line = value.toString();
         
         try {
            String[] parts = line.split("\t");
            String comp_key = parts[0];
            String votes = parts[1];
            String[] parts1 = comp_key.split("_");
            String post_id = parts1[0];
            context.write(new IntWritable(Integer.parseInt(votes)), new IntWritable(Integer.parseInt(post_id)));
               
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }
   
   @SuppressWarnings("rawtypes")
   public static class DescendingKeyComparator extends WritableComparator {
      protected DescendingKeyComparator() {
          super(IntWritable.class, true);
      }

      @Override
      public int compare(WritableComparable w1, WritableComparable w2) {
         IntWritable key1 = (IntWritable) w1;
         IntWritable key2 = (IntWritable) w2;          
         return -1 * key1.compareTo(key2);
      }
   }
   
   public static void main(String[] args) throws Exception {
      if(args.length != 2){
         System.err.println("Usage : Query1 <inputPath> <output Path>");
         System.exit(-1);
      }
      ToolRunner.run(new Configuration(), new Query1(), args);
      
	}

   private String OUTPUT_PATH = "intermediate_output";
   
   @SuppressWarnings("deprecation")
   @Override
   public int run(String[] args) throws Exception {
      Random rand = new Random();
      int randomNum = rand.nextInt((100 - 1) + 1) + 1;
      OUTPUT_PATH = OUTPUT_PATH + String.valueOf(randomNum);
      
      Configuration conf = getConf();
      Job job = new Job(conf, "Query1");
      
      job.setJarByClass(Query1.class);
      job.setJobName("Math Stack Query1");
      
      job.setMapperClass(QueryMapper.class);
      job.setCombinerClass(QueryReducer.class);
      job.setReducerClass(QueryReducer.class);
      
      job.setPartitionerClass(KeyPartitioner.class);
      job.setGroupingComparatorClass(KeyGroupingComparator.class);
      job.setSortComparatorClass(CompositeKeyComparator.class);
      
      job.setMapOutputKeyClass(StockKey.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(StockKey.class);
      job.setOutputValueClass(IntWritable.class);
      
      TextInputFormat.addInputPath(job, new Path(args[0]));
      TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
      
      job.waitForCompletion(true);
      
      Configuration conf2 = getConf();
      Job job2 = new Job(conf2, "Query1-2");
      job2.setJarByClass(Query1.class);

      job2.setMapperClass(QueryMapper2.class);
      //job2.setReducerClass(QueryReducer2.class);
      
      job2.setMapOutputKeyClass(IntWritable.class);
      job2.setMapOutputValueClass(IntWritable.class);

      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(IntWritable.class);
      job2.setSortComparatorClass(DescendingKeyComparator.class);
      
      TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
      TextOutputFormat.setOutputPath(job2, new Path(args[1]));
      
      return job2.waitForCompletion(true) ? 0 : 1;
   }
}
