import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class SymptomCounter {

    public static class DiseaseWordsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        private final Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException{
            loadStopWords();
        }

        // Map Lines of Symptom2Disease.csv to key value pairs
        // Key = Disease name | Value = Symptom word
        // format of csv is like so: [rowID, diseaseName, textSymptomDescription]
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            String diseaseName = parts[1];
            String desc = parts[2];

            // Remove unnecessary quotations from beginning and end of desc.
            String symptomDescription = desc.replaceAll("^\"|\"$", "");

            // Convert the string description into a series of word 'tokens'
            StringTokenizer tokenizer = new StringTokenizer(symptomDescription.toLowerCase());

            // Ensure that the token is not a stop word (this, I, a etc.)
            // Then add it to the next output for the current key
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (!stopWords.contains(token)) {
                    outputKey.set(diseaseName);
                    outputValue.set(token);
                    context.write(outputKey, outputValue);
                }
            }
        }

        // A method to load the stop words text file from resources
        // uses BufferedReader to read each line for a single stop word.
        private void loadStopWords() throws IOException {
            ClassLoader classLoader = getClass().getClassLoader();

            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                    Objects.requireNonNull(classLoader.getResourceAsStream("stop_words_english.txt"))))) {

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }
    }


    public static class DiseaseWordsReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputValue = new Text();

        // Reduce the output of mappers (diseaseName: [textDescription])
        // Finds the 10 most common description words for the key (disease)
        // Returns [disease     topWord1: count, topWord2: count, topWord3: count]
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Create a map to contain each unique word and the count of its occurrences.
            HashMap<String, Integer> map = new HashMap<>();

            // for each text value, add to map if new or +1 count
            for (Text value : values) {
                map.put(value.toString(), map.getOrDefault(value.toString(), 0) + 1);
            }

            // Create a sorted list of map key:value entries, sorted by word count (descending order)
            List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(map.entrySet());
            sortedEntries.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

            // Now remove all but the top 10, and add to a list of maps.
            // format of this list will be [ {word1: count}, {word2:count}, {word3:count} ]
            // If num topWords < 10, then only add num topWords
            List<Map<String, Integer>> topWordsList = new ArrayList<>();
            for (int i = 0; i < Math.min(10, sortedEntries.size()); i++) {
                Map.Entry<String, Integer> entry = sortedEntries.get(i);
                Map<String, Integer> wordMap = new HashMap<>();
                wordMap.put(entry.getKey(), entry.getValue());
                topWordsList.add(wordMap);
            }

            // Build the result string to be will be used as output
            // Creates a string description of the top words and their counts
            StringBuilder resultBuilder = new StringBuilder();
            for (Map<String, Integer> m : topWordsList) {
                for (String k: m.keySet()) {
                    resultBuilder.append(m.get(k)).append(":").append(k).append(",");
                }
            }

            outputValue.set(resultBuilder.toString());
            context.write(key, outputValue);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Don't run code if it does not receive 2 arguments for input/output
        if (otherArgs.length != 2) {
            System.err.println("Error: Wrong arg number. Provide input source and output location");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Symptom Counter");
        job.setJarByClass(SymptomCounter.class);
        job.setMapperClass(DiseaseWordsMapper.class);
        job.setReducerClass(DiseaseWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean status = job.waitForCompletion(true);

        if (status) {
            System.exit(0);
        } else {
            System.exit(2);
        }
;    }
}
