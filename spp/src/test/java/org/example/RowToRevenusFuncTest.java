package org.example;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.naissance.beans.Revenus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class RowToRevenusFuncTest {


    @Test
    public void testApply() {
        RowToRevenusFunc f = new RowToRevenusFunc();
        StructType shema =new StructType(
                new StructField[]{
                        new StructField(
                                "type_de_travailleur_independant",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "secteur_d_activite",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "annee",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "nombre_de_ti",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "revenu",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),

                }
        );
        String[] value=new String[]{"Autoentrepreneur", "AZ - Agriculture, sylviculture et pêche","2015","2010","8208039"};
        Row s = new GenericRowWithSchema(value, shema);
        Row r1 = RowFactory.create("a","b","c","d","e" );
        Revenus expected= Revenus.builder()
                .type_de_travailleur_independant("Autoentrepreneur")
                .secteur_d_activite("AZ - Agriculture, sylviculture et pêche")
                .annee("2015")
                .nombre_de_ti("2010")
                .revenu("8208039")
                .build();
        Revenus actual = f.apply(s);

        assertThat(actual).isEqualTo(expected);
    }
}