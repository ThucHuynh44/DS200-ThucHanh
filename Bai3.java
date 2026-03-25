import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    // ---------------------------------------------------------
    // 1. MAPPER CHO MOVIES.TXT
    // ---------------------------------------------------------
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("MovieID") || line.trim().isEmpty()) return;

            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            if (parts.length >= 2) {
                outKey.set(parts[0].trim());           
                outValue.set("M_" + parts[1].trim());  
                context.write(outKey, outValue);
            }
        }
    }

    // ---------------------------------------------------------
    // 2. MAPPER CHO RATINGS.TXT (Đọc kèm Users.txt từ Cache)
    // ---------------------------------------------------------
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        
        // Bảng băm để lưu trữ UserID -> Gender
        private Map<String, String> userGenderMap = new HashMap<>();

        // Hàm setup() đọc file users.txt từ Cache
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("UserID") || line.trim().isEmpty()) continue;
                    String delim = line.contains("::") ? "::" : ",";
                    String[] parts = line.split(delim);
                    if (parts.length >= 2) {
                        String userID = parts[0].trim();
                        String gender = parts[1].trim().toUpperCase(); 
                        userGenderMap.put(userID, gender);
                    }
                }
                br.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("UserID") || line.trim().isEmpty()) return;

            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            if (parts.length >= 3) {
                String userID = parts[0].trim();
                String movieID = parts[1].trim();
                String rating = parts[2].trim();
                
                String gender = userGenderMap.getOrDefault(userID, "Unknown");

                if (!gender.equals("Unknown")) {
                    outKey.set(movieID);
                    outValue.set("R_" + rating + "_" + gender);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // ---------------------------------------------------------
    // 3. REDUCER
    // ---------------------------------------------------------
    public static class GenderReducer extends Reducer<Text, Text, Text, NullWritable> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";
            double sumMale = 0.0, sumFemale = 0.0;
            int countMale = 0, countFemale = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M_")) {
                    movieTitle = v.substring(2); 
                } else if (v.startsWith("R_")) {
                    String[] parts = v.split("_"); 
                    if (parts.length == 3) {
                        try {
                            double rating = Double.parseDouble(parts[1]);
                            String gender = parts[2].toUpperCase();
                            
                            if (gender.equals("M")) {
                                sumMale += rating;
                                countMale++;
                            } else if (gender.equals("F")) {
                                sumFemale += rating;
                                countFemale++;
                            }
                        } catch (NumberFormatException e) { }
                    }
                }
            }

            if (countMale > 0 || countFemale > 0) {
                double maleAvg = countMale > 0 ? (sumMale / countMale) : 0.0;
                double femaleAvg = countFemale > 0 ? (sumFemale / countFemale) : 0.0;

                // SỬA LẠI FORMAT CHUẨN Ở ĐÂY: MovieTitle Male: xx, Female: yy
                String resultStr = String.format(Locale.US, "%s Male: %.2f, Female: %.2f", movieTitle, maleAvg, femaleAvg);
                context.write(new Text(resultStr), NullWritable.get());
            }
        }
    }

    // ---------------------------------------------------------
    // 4. MAIN DRIVER
    // ---------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: hadoop jar Bai3.jar Bai3 <movies_path> <ratings_path> <users.txt_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analyzer");
        job.setJarByClass(Bai3.class);

        // Đưa file users.txt vào Cache
        job.addCacheFile(new Path(args[2]).toUri());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(GenderReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}