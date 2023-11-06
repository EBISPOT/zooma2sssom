import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper


class SPARQLEndpoint():
    """
    SPARQL Endpoint to query ZOOMA triplestore
    """
    def __init__(self):
        self.sparql = SPARQLWrapper("http://ves-pg-7b:8892/sparql")

    def sssom_metadata(self, graph):
        """
        Query SSSOM values by graph
        """
        return f"""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX ZOOMA_TERMS: <http://rdf.ebi.ac.uk/terms/zooma/>
            PREFIX open: <http://www.openannotation.org/ns/>
            PREFIX semapv: <https://w3id.org/semapv/vocab/>

            SELECT DISTINCT (STR(?label) as ?literal) ?predicate_id ?object_id
                            ?mapping_justification ?mapping_provider
                            (STR(?author_l) AS ?author_label) ?mapping_date
            FROM {graph}
            WHERE {{
                ?s rdf:type open:DataAnnotation .
                ?s open:hasBody ?object_id .
                ?s open:hasBody ?zooma_literal . 
                ?s <http://purl.org/dc/elements/1.1/source> ?mapping_provider .
                OPTIONAL {{ ?s open:annotated ?mapping_date . }}
                OPTIONAL {{ ?s open:annotator ?author_l . }}

                ?object_id rdf:type open:SemanticTag .
                ?zooma_literal rdf:type ZOOMA_TERMS:Property .
                ?zooma_literal ZOOMA_TERMS:propertyValue ?label .

                BIND(rdfs:label AS ?predicate_id)
                BIND(semapv:ManualMappingCuration AS ?mapping_justification)
            }}
        """

    def query_endpoint(self, query):
        """
        Query SPARQL endpoint
        """
        self.sparql.setReturnFormat(JSON)
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results

    def transform_result(self, result):
        """
        Transform query result to get only value
        """
        res = []
        for row in result["results"]["bindings"]:
            a = {}
            for key, value in row.items():
                a[key] = value["value"]
            res.append(a)
        return pd.DataFrame(res)
