package org.example.naissance.beans;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
@Data
@Getter
@Builder
public class Revenus implements Serializable {

    private String type_de_travailleur_independant;
    private String secteur_d_activite;
    private String annee;
    private String nombre_de_ti;
    private String revenu;

}