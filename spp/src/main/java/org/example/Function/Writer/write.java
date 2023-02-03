package org.example.Function.Writer;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.example.naissance.beans.Revenus;

import java.util.function.Consumer;
@RequiredArgsConstructor
public class write implements Consumer<Dataset<Revenus>> {
    private  final String outputPath ;

    @Override
    public void accept(Dataset<Revenus> ActeNaissanceDataset) {
        ActeNaissanceDataset.write().mode(SaveMode.Overwrite).csv(outputPath);
    }
}
