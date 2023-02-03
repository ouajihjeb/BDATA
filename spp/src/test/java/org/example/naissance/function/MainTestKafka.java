package org.example.naissance.function;


import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.MainKafka;
import org.example.StreamMain;
import org.example.reader.HdfsTextFileReader;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MainTestKafka {

    String outputPathCSv = ConfigFactory.load().getString("app.path.output");

    @Test
    public void test() throws IOException, InterruptedException {
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        log.info("fileSystem used in the test : localFs.getScheme = {}", localFs.getScheme());

        MainKafka.main(new String[0]);

        Path outputPath = new Path(outputPathCSv);
        Stream<Path> CsvvFilePaths = Arrays.stream(localFs.listStatus(outputPath))
                .map(FileStatus::getPath)
                .filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".csv"));

        List<String> lines = CsvvFilePaths
                .flatMap(outputPathCSv -> new HdfsTextFileReader(localFs, outputPathCSv).get())
                .collect(Collectors.toList());

        assertThat(lines)
                .isNotEmpty();

    }
}

