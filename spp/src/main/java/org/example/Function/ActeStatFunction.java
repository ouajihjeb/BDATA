package org.example.Function;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.Revenus;
import org.example.RevenusMapper;

import java.util.function.Function;

@RequiredArgsConstructor
public class ActeStatFunction implements Function<Dataset<Row>, Dataset<Revenus> > {



    @Override
    public Dataset<Revenus> apply(Dataset<Row> rowDataset) {

        return  new RevenusMapper().apply(rowDataset);
    }
}
