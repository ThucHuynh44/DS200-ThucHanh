import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    // Ký tự phân cách của tập dữ liệu
    private static final String DELIMITER = ",";

    // ---------------------------------------------------------
    // 1. MAPPER CHO MOVIES.TXT
    // ---------------------------------------------------------
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("MovieID") || line.trim().isEmpty()) return;

            // Tự động nhận diện dấu phân cách
            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            // Lấy phần tử cuối cùng làm thể loại (phòng trường hợp Tên phim có chứa dấu phẩy)
            if (parts.length >= 3) {
                String movieID = parts[0].trim();
                String genresStr = parts[parts.length - 1].trim(); 
                
                outKey.set(movieID);
                
                // Tách riêng từng thể loại bằng dấu "|" và gửi đi
                String[] genres = genresStr.split("\\|");
                for (String genre : genres) {
                    outValue.set("M_" + genre.trim());
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // ---------------------------------------------------------
    // 2. MAPPER CHO RATINGS.TXT
    // ---------------------------------------------------------
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("UserID") || line.trim().isEmpty()) return;

            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            if (parts.length >= 3) {
                outKey.set(parts[1].trim());           // Key: MovieID
                outValue.set("R_" + parts[2].trim());  // Value: R_Rating
                context.write(outKey, outValue);
            }
        }
    }

    // ---------------------------------------------------------
    // 3. REDUCER
    // ---------------------------------------------------------
    public static class GenreReducer extends Reducer<Text, Text, Text, NullWritable> {
        // TreeMap để lưu trữ tự động theo thứ tự A-Z. 
        // Mảng double[] chứa 2 giá trị: index 0 là Tổng điểm, index 1 là Số lượt đánh giá
        private Map<String, double[]> genreStats = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movieGenres = new ArrayList<>();
            double sumRating = 0.0;
            int count = 0;

            // Tách dữ liệu ra từ các Mapper gửi tới
            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M_")) {
                    movieGenres.add(v.substring(2)); // Thu thập các thể loại của phim này
                } else if (v.startsWith("R_")) {
                    try {
                        sumRating += Double.parseDouble(v.substring(2)); // Thu thập điểm
                        count++;
                    } catch (NumberFormatException e) { }
                }
            }

            // Nếu phim có người đánh giá và có khai báo thể loại
            if (count > 0 && !movieGenres.isEmpty()) {
                // Cộng dồn điểm và số lượt đánh giá vào bảng thống kê toàn cục cho từng thể loại
                for (String genre : movieGenres) {
                    // Nếu thể loại này chưa có trong Map, tạo mới với tổng=0, count=0
                    double[] stats = genreStats.getOrDefault(genre, new double[]{0.0, 0.0});
                    stats[0] += sumRating; // Cộng tổng điểm
                    stats[1] += count;     // Cộng tổng lượt
                    genreStats.put(genre, stats);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sau khi Hadoop xử lý xong mọi phim, tiến hành tính trung bình và in ra kết quả
            for (Map.Entry<String, double[]> entry : genreStats.entrySet()) {
                String genre = entry.getKey();
                double totalSum = entry.getValue()[0];
                int totalCount = (int) entry.getValue()[1];
                
                if (totalCount > 0) {
                    double avgRating = totalSum / totalCount;
                    // Format đúng chuẩn yêu cầu: Genre: AverageRating (TotalRatings)
                    String resultStr = String.format(Locale.US, "%s Avg:%.2f, Count: %d", genre, avgRating, totalCount);
                    context.write(new Text(resultStr), NullWritable.get());
                }
            }
        }
    }

    // ---------------------------------------------------------
    // 4. MAIN DRIVER
    // ---------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analyzer");
        job.setJarByClass(Bai2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(GenreReducer.class);
        job.setNumReduceTasks(1); // Gom hết vào 1 cục để cleanup in ra danh sách cuối cùng

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}