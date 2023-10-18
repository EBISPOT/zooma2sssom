import pandas as pd
from SPARQLWrapper import TSV, SPARQLWrapper


class SPARQLEndpoint():
    def __init__(self):
        self.sparql = SPARQLWrapper("http://ves-pg-7b:8892/sparql")
        
    def sssom_metadata(self, graph):
        return f"""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX ZOOMA_TERMS: <http://rdf.ebi.ac.uk/terms/zooma/>
            PREFIX open: <http://www.openannotation.org/ns/>

            SELECT DISTINCT (STR(?label) as ?literal) ?predicate_id ?object_id ?mapping_justification ?mapping_provider (STR(?author_l) AS ?author_label) ?mapping_date 
            FROM {graph}
            WHERE {{
                ?s rdf:type open:DataAnnotation .
                ?s open:hasBody ?object_id .
                ?s open:hasBody ?zooma_literal . 
                ?s <http://purl.org/dc/elements/1.1/source> ?mapping_provider .
                OPTIONAL {{ ?s <http://www.openannotation.org/ns/annotated> ?mapping_date . }}
                OPTIONAL {{ ?s <http://www.openannotation.org/ns/annotator> ?author_l . }}

                ?object_id rdf:type <http://www.openannotation.org/ns/SemanticTag> .
                ?zooma_literal rdf:type <http://rdf.ebi.ac.uk/terms/zooma/Property> .
                ?zooma_literal ZOOMA_TERMS:propertyValue ?label .

                BIND(rdfs:label AS ?predicate_id)
                BIND(<https://w3id.org/semapv/vocab/ManualMappingCuration> AS ?mapping_justification)
            }} LIMIT 100
        """
    
    def query_endpoint(self, query):
        self.sparql.setReturnFormat(TSV)
        self.sparql.setQuery(query)
        
        results = self.sparql.query().convert()
        return str(results)
    
    def transform_result(self, result):
        return pd.DataFrame([r.split("\\t") for r in result.split("\\n")[1:]], columns=[c for c in result.split("\\n")[0].split("\\t")])