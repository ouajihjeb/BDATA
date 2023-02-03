package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.Revenus;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

public class RevenusStatFunction implements Function<Dataset<Row>, Dataset<Revenus> > {
    @Override
    public  Dataset<Revenus> apply(Dataset<Row> rowDataset) {
        Dataset<Revenus> cleanDs = new RevenusMapper().apply(rowDataset) ;
      //  cleanDs.printSchema();
      //  cleanDs.show(5, false);
      //  Dataset<Row>statds =  cleanDs.groupBy("annee").agg(count("type_de_travailleur_independant").as("nb")  );
       // statds.show();

        return cleanDs;
    }
}