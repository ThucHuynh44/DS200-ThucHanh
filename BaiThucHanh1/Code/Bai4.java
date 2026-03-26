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

public class Bai4 {

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
        
        // Bảng băm để lưu trữ UserID -> Nhóm tuổi (AgeGroup)
        private Map<String, String> userAgeGroupMap = new HashMap<>();

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
                    
                    // Lấy Age ở cột 3 (index 2)
                    if (parts.length >= 3) {
                        String userID = parts[0].trim();
                        try {
                            int age = Integer.parseInt(parts[2].trim());
                            String ageGroup = "";
                            
                            // Phân loại nhóm tuổi
                            if (age <= 18) {
                                ageGroup = "0-18";
                            } else if (age <= 35) {
                                ageGroup = "18-35";
                            } else if (age <= 50) {
                                ageGroup = "35-50";
                            } else {
                                ageGroup = "50+";
                            }
                            
                            userAgeGroupMap.put(userID, ageGroup);
                        } catch (NumberFormatException e) {
                            // Bỏ qua nếu tuổi không phải là số
                        }
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
                
                String ageGroup = userAgeGroupMap.getOrDefault(userID, "Unknown");

                if (!ageGroup.equals("Unknown")) {
                    outKey.set(movieID);
                    // Value gửi đi có dạng: R_Rating_AgeGroup (VD: R_4.5_18-35)
                    outValue.set("R_" + rating + "_" + ageGroup);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // ---------------------------------------------------------
    // 3. REDUCER
    // ---------------------------------------------------------
    public static class AgeGroupReducer extends Reducer<Text, Text, Text, NullWritable> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";
            
            // Khởi tạo mảng lưu trữ Tổng điểm và Số lượt đánh giá cho 4 nhóm
            double sum0_18 = 0.0, sum18_35 = 0.0, sum35_50 = 0.0, sum50_plus = 0.0;
            int count0_18 = 0, count18_35 = 0, count35_50 = 0, count50_plus = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M_")) {
                    movieTitle = v.substring(2); 
                } else if (v.startsWith("R_")) {
                    String[] parts = v.split("_"); 
                    if (parts.length == 3) {
                        try {
                            double rating = Double.parseDouble(parts[1]);
                            String ageGroup = parts[2];
                            
                            switch (ageGroup) {
                                case "0-18":
                                    sum0_18 += rating;
                                    count0_18++;
                                    break;
                                case "18-35":
                                    sum18_35 += rating;
                                    count18_35++;
                                    break;
                                case "35-50":
                                    sum35_50 += rating;
                                    count35_50++;
                                    break;
                                case "50+":
                                    sum50_plus += rating;
                                    count50_plus++;
                                    break;
                            }
                        } catch (NumberFormatException e) { }
                    }
                }
            }

            // Chỉ xuất ra kết quả nếu phim này có ít nhất 1 người đánh giá
            if (count0_18 > 0 || count18_35 > 0 || count35_50 > 0 || count50_plus > 0) {
                
                // Chuyển đổi thành String: Nếu có điểm thì format "%.2f", nếu không thì xuất "NA"
                String str0_18 = count0_18 > 0 ? String.format(Locale.US, "%.2f", sum0_18 / count0_18) : "NA";
                String str18_35 = count18_35 > 0 ? String.format(Locale.US, "%.2f", sum18_35 / count18_35) : "NA";
                String str35_50 = count35_50 > 0 ? String.format(Locale.US, "%.2f", sum35_50 / count35_50) : "NA";
                String str50_plus = count50_plus > 0 ? String.format(Locale.US, "%.2f", sum50_plus / count50_plus) : "NA";

                // Format chuỗi sử dụng %s (dành cho String) thay vì %.2f
                String resultStr = String.format(Locale.US, 
                    "%s 0-18: %s 18-35: %s 35-50: %s 50+: %s", 
                    movieTitle, str0_18, str18_35, str35_50, str50_plus);
                
                context.write(new Text(resultStr), NullWritable.get());
            }
        }
    }
    // ---------------------------------------------------------
    // 4. MAIN DRIVER
    // ---------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: hadoop jar Bai4.jar Bai4 <movies_path> <ratings_path> <users.txt_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analyzer");
        job.setJarByClass(Bai4.class);

        // Nạp file users.txt vào bộ nhớ đệm
        job.addCacheFile(new Path(args[2]).toUri());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(AgeGroupReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}