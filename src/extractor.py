from sparql_endpoint import SPARQLEndpoint


def process(df):
    # TODO process the table to remove the double quotes
    return df

def main():    
    query = SPARQLEndpoint()
    
    graphs = {
        "atlas": "<http://rdf.ebi.ac.uk/resource/zooma/atlas>",
        "hca": "<http://rdf.ebi.ac.uk/resource/zooma/HCA>",
        "gwas": "<http://rdf.ebi.ac.uk/resource/zooma/GWAS>",
        "cbi": "<http://rdf.ebi.ac.uk/resource/zooma/cbi>",
        "ebi-biosamples": "<http://rdf.ebi.ac.uk/resource/zooma/EBI-BioSamples>",
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
        # df = process(df)
        df.to_csv(f"./mappings/{name}.zooma.sssom.tsv", sep="\t", index=False)

if __name__ == "__main__":
    main()