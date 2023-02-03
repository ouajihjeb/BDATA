package org.example;

import org.apache.spark.sql.Row;
import org.example.naissance.beans.Revenus;

import java.io.Serializable;
import java.util.function.Function;

public class RowToRevenusFunc implements Function<Row, Revenus> , Serializable {
    @Override
    public Revenus apply(Row row) {
        //  String rowValue = row.getString(0) ;   //position de la ligne


        String type_de_travailleur_independant = row.getAs("type_de_travailleur_independant") ; //nom de colonne que j vais recuperer ? Comment parcourir?
        String secteur_d_activite = row.getAs("secteur_d_activite") ;
        String  annee = row.getAs("annee") ;
        String nombre_de_ti = row.getAs("nombre_de_ti") ;
        String revenu = row.getAs("revenu") ;


/*

        //String[] nomPrenoms = line.substring(0, 81).split("\\*", 2);
        String[] nomPrenoms = StringUtils.splitByWholeSeparatorPreserveAllTokens(line.substring(0, 81), "*", 2);

        String nom = nomPrenoms[0];
        String prenom = nomPrenoms[1];
*/
        return Revenus.builder()
                .type_de_travailleur_independant(type_de_travailleur_independant)
                .secteur_d_activite(secteur_d_activite)
                .annee(annee)
                .nombre_de_ti(nombre_de_ti)
                .revenu(revenu)
                .build();
    }
}
