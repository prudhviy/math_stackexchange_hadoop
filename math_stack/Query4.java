package math_stack;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class Query4 extends Configured implements Tool {
   
   public static class QueryMapper extends Mapper<LongWritable, Text, Text, Text>{

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String json_line = value.toString();
         String patternString = "thank you";
         Pattern pattern = Pattern.compile(patternString);
         
         try {
            JSONParser parser = new JSONParser();
            JSONObject json_obj;
            json_obj = (JSONObject) parser.parse(json_line);
            String doc_type = (String) json_obj.get("type");
            
            if(doc_type.equals("comments")) {
               String comment_id = (String) json_obj.get("Id");
               String comment_text = (String) json_obj.get("Text");
               Matcher matcher = pattern.matcher(comment_text);
               boolean matches = matcher.matches();
               
               if(matches) {
                  context.write(new Text(comment_id), new Text(comment_text));
               }
            }
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }  
   
   public static void main(String[] args) throws Exception {
      if(args.length != 2) {
         System.err.println("Usage : Query4 <inputPath> <output Path>");
         System.exit(-1);
      }
      
      ToolRunner.run(new Configuration(), new Query4(), args);   
   }
   
   @SuppressWarnings("deprecation")
   public int run(String[] args) throws Exception {
      Configuration conf = getConf();
      conf.setInt(MRJobConfig.NUM_MAPS, 10);
      conf.setInt(MRJobConfig.NUM_REDUCES, 4);
      
      Job job = new Job(conf, "Query4");
      
      job.setJarByClass(Query4.class);
      job.setJobName("Q4 " + args[1]);
      
      job.setMapperClass(QueryMapper.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      
      TextInputFormat.addInputPath(job, new Path(args[0]));
      TextOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.waitForCompletion(true);

      return 0;
   }
}
