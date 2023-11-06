from datetime import datetime

from sparql_endpoint import SPARQLEndpoint

# from sssom.io import annotate_file


def annotate_sssom(df):
    """
    Add mapping_set annotations and curie_map to the csv file
    using sssom method
    """
    # TODO : add mapping_set annotations and curie_map
    # df = annotate_file()
    return df


def process_date(df):
    """
    Transform date value
    """
    date_column = df["mapping_date"]

    def fix_year(date):
        date_format = "%Y-%m-%dT%H:M:S"
        try:
            datetime.strptime(date, date_format)
        except ValueError:
            year = date[:3].replace("00", "20")
            date = f"{year}{date[3:]}"
        return date

    df["mapping_date"] = date_column.apply(fix_year)
    return df


def main():
    """
    Create SSSOM TSV files for each graph in endpoint
    """
    query = SPARQLEndpoint()
    graphs = {
        "atlas": "<http://rdf.ebi.ac.uk/resource/zooma/atlas>",
        "hca": "<http://rdf.ebi.ac.uk/resource/zooma/HCA>",
        "gwas": "<http://rdf.ebi.ac.uk/resource/zooma/GWAS>",
        "cbi": "<http://rdf.ebi.ac.uk/resource/zooma/cbi>",
        "ebi-biosamples": """
            <http://rdf.ebi.ac.uk/resource/zooma/EBI-BioSamples>
        """,
        "faang": "<http://rdf.ebi.ac.uk/resource/zooma/FAANG>",
        "clinvar-xrefs": "<http://rdf.ebi.ac.uk/resource/zooma/clinvar-xrefs>",
        "cttv": "<http://rdf.ebi.ac.uk/resource/zooma/cttv>",
        "ebisc": "<http://rdf.ebi.ac.uk/resource/zooma/ebisc>",
        "eva-clinvar": "<http://rdf.ebi.ac.uk/resource/zooma/eva-clinvar>",
        "metabolights": "<http://rdf.ebi.ac.uk/resource/zooma/metabolights>",
        "sysmicro": "<http://rdf.ebi.ac.uk/resource/zooma/sysmicro>",
        "ukbiobank": "<http://rdf.ebi.ac.uk/resource/zooma/ukbiobank>",
        "uniprot": "<http://rdf.ebi.ac.uk/resource/zooma/uniprot>",
    }

    for name, graph in graphs.items():
        print(f"Querying graph {graph}")
        sssom = query.query_endpoint(query.sssom_metadata(graph=graph))
        print("Transforming results...")
        df = query.transform_result(sssom)
        if "mapping_date" in df.columns:
            df = process_date(df)
        df.to_csv(f"./mappings/{name}.zooma.sssom.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
