from pyspark.sql import functions as F


def clean_phone(col_name):
    """
    This function define to clean number phone and is filtering when the field is Null. Otherwise do a regexp_replace on col_name.
    """
    return (
        F.when(
            F.col(col_name).isNull(),
            F.lit("Unknown")
        )
        .otherwise(
            F.regexp_replace(
                F.regexp_replace(F.col(col_name), r"^\+\d{1,3}\s", ""),
                # ^ = inizio stringa; \+ = il simbolo + presente nel prefisso; \d = una cifra; {1,3} = cifra ripetuta 1, 2 o 3 volte; \s = uno spazio
                # Trovare una stringa che inizia con il simbolo +, seguito da 1-3 cifre, seguito da uno spazio — e rimuovila
                r"[^\d]", ""
                # [ ] = Insieme di caratteri; ^ dentro [] = Negazione — "tutto tranne..."; \d = Cifre 0-9
                # Trova tutti i caratteri che NON sono cifre e sostituiscili con niente "" (ad es. partendo da 333 - 5555, si ottiene 3335555)
            )
        )
    )


def fill_null(col_name, fill_value):
    return (
        F.when(
            F.col(col_name).isNull(),
            F.lit(fill_value)
        )
        .otherwise(F.col(col_name))
    )
