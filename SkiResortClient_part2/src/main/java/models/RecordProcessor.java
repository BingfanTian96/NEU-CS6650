package models;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * The type Record processor.
 *
 * @className: RecordProcessor
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10 /6/22 2:45 AM
 */
public class RecordProcessor {

    private Queue<Record> records;
    private long throughPut;

    /**
     * Instantiates a new Record processor.
     *
     * @param records    the records
     * @param throughPut the through put
     */
    public RecordProcessor(Queue<Record> records, long throughPut) {
        this.records = records;
        this.throughPut = throughPut;
    }

    /**
     * Process the results to CSV file.
     */
    public void process(){
        writeToCSV();
        printPart2Results();
    }

    private void printPart2Results() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        List<Long> latencies = new ArrayList<>();
        for (Record record: records){
            stats.addValue(record.getLatency());
            latencies.add(record.getLatency());
        }
        System.out.println("\n\nClient Part 2 Result:");
        System.out.println("-----------------------------------------------");
        System.out.println("Mean response time: " + stats.getMean() + " milliseconds");
        System.out.println("Median response time: " + stats.getPercentile(50) + " milliseconds");
        System.out.println("Throughput: " + throughPut + " requests/second");
        System.out.println("P99: " + stats.getPercentile(99) + " milliseconds");
        System.out.println("Min: " + stats.getMin() + " milliseconds; Max: " + stats.getMax() + " milliseconds");
        Collections.sort(latencies);
    }

    private void writeToCSV(){
        try {
            FileWriter csvWriter = new FileWriter( "records.csv");
            csvWriter.append("startTime,requestType,latency,responseCode\n");
            for (Record record : records) {
                csvWriter.append(record.toCSVFormat());
            }
            csvWriter.flush();
            csvWriter.close();
        }
        catch (IOException e) {
            System.out.println("Write to CSV failed!");
        }
    }
}
