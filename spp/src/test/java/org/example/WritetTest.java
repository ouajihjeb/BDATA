package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import org.example.Writer.CsvWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.naissance.beans.Revenus;

@Slf4j
public class WritetTest {

    @Test
    public void readerTest() throws IOException {
        Encoder<Revenus> RevenusEncoder = Encoders.bean(Revenus.class);
        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        Dataset<Revenus> javaBeanDS = spark.createDataset(
                Collections.singletonList(Revenus.builder()          //singleton
                        .type_de_travailleur_independant("01500")
                        .secteur_d_activite("AMBERIEU EN BUGEY")
                        .annee("01004")
                        .nombre_de_ti("3")
                        .revenu("200")
                        .build()),
                RevenusEncoder
        );
        javaBeanDS.show();

        Config config = ConfigFactory.load("application.conf") ;
        String outputPath = config.getString("app.path.output") ;

        CsvWriter csvWriter = new CsvWriter(outputPath) ;

        csvWriter.accept(javaBeanDS);


        Path output = Paths.get(outputPath);
        Stream<Path> jsonFilePaths = Files.list(output)
                .filter(p -> p.getFileName().toString().startsWith("part-") && p.toString().endsWith(".csv"))
                ;
        List<String> lines=jsonFilePaths
                .flatMap(
                        outputJsonfilepath ->{
                            Stream<String> jsonFileContent= Stream.empty();
                            try {
                                jsonFileContent=Files.lines(outputJsonfilepath);
                            }
                            catch (IOException e)
                            {
                                log.info("ccc");
                            }
                            return jsonFileContent;

                        }
                )
                .collect(Collectors.toList());
        assertThat(lines)
                .isNotEmpty()
               .contains("01004,3,200,AMBERIEU EN BUGEY,01500")


        ;

    }


}
