package org.example.Writer;


import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.codehaus.commons.compiler.java8.java.util.function.Consumer;
import org.example.naissance.beans.Revenus;

@RequiredArgsConstructor

public class CsvWriter implements Consumer<Dataset<Revenus>> {

    private  final String outputPath ;

    @Override
    public void accept(Dataset<Revenus> revenusDataset) {
        revenusDataset.write().mode(SaveMode.Overwrite).csv(outputPath);



    }
}
