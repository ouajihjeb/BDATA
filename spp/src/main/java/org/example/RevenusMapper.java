package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.Revenus;

import java.util.function.Function;

public class RevenusMapper implements Function<Dataset<Row>, Dataset<Revenus>> {
    private final RowToRevenusFunc parser = new RowToRevenusFunc(); //

    //map function dans spark == function dans java
    private final MapFunction<Row, Revenus> task = parser::apply;  //task object containt data from text(ROW?)?
    //ou bien //  private final MapFunction<Row, ActeDeces> task = r->parser.apply(r);


    @Override
    public Dataset<Revenus> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(Revenus.class));
    }

    //stream data set . map et on passe font excecuter a chaque element de dataset (task) la tache va produit des objet
    //task (tache) sexecute dans executer ( driver envoie ma tache au executer et ma tache apply doit serializable

}

