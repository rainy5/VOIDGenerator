
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Bag;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.vocabulary.RDF;

import static com.hp.hpl.jena.vocabulary.RDF.Bag;
import static com.hp.hpl.jena.vocabulary.RDF.Seq;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Hongyan Wu
 */
public class Bio2RDFMetaGen {

     private static final String VOID_PREFIX = "PREFIX void: <" + VOID2.NAMESPACE + ">\n";
      private static final String DS_PREFIX = "PREFIX ds:  <http://bio2rdf.org/bio2rdf.dataset_vocabulary:>\n";
      
    public static final String propertyPartionQuery = VOID_PREFIX+DS_PREFIX
              
            + "SELECT ?property ?subject_count ?subject_distinct_count ?object_distinct_count\n"
            + "{ \n"
            + "[] void:subset [\n"
            + "   a ds:Dataset-Subject-Property-Object-Count; \n"
            + "   void:linkPredicate ?property;\n"
            + "   void:subjectsTarget [\n"
            + "     void:entities ?subject_count; \n"
            + "     void:distinctEntities ?subject_distinct_count;\n"
            + "  ];\n"
            + "   void:objectsTarget [\n"
            + "     void:entities ?object_count; \n"
            + "     void:distinctEntities ?object_distinct_count;\n"
            + "  ]]\n"
            + "}";
   
    private static final String classPartionQuery = VOID_PREFIX+  "PREFIX ds: <http://bio2rdf.org/bio2rdf.dataset_vocabulary:>\n"+
            "SELECT *\n" +
" { [] void:subset [ \n" +
"       a ds:Dataset-Type-Count; \n" +
"       void:class ?type; \n" +
"       void:entities ?count; \n" +
"       void:distinctEntities ?distinctCount;\n" +
"   ]\n" +
" }";
   
   public static final String distEntiryQuery = VOID_PREFIX+DS_PREFIX+ "SELECT * { [] void:subset [ a ds:Dataset-Distinct-Entities; void:entities ?entities;]}";
    public static final String tripleQuery = VOID_PREFIX+DS_PREFIX+"SELECT * { [] void:subset [ a ds:Dataset-Triples; void:entities ?triples;]}";
    public static final String distSubjQuery = VOID_PREFIX+DS_PREFIX+"SELECT * { [] void:subset [ a ds:Dataset-Distinct-Subjects; void:entities ?subjects;]}";
  public static final String distObjQuery = VOID_PREFIX+DS_PREFIX+ "SELECT * { [] void:subset [ a ds:Dataset-Distinct-Objects; void:entities ?objects;]}";
   public static final String distPropQuery = VOID_PREFIX+DS_PREFIX+" SELECT * { [] void:subset [ a ds:Dataset-Distinct-Properties; void:entities ?properties;]}";
   
    /**
     * @param args
     * arg[0]: the endpoint url string
     * arg[1]: the directory of the output void file
     */
    public static void main(String[] args) throws IOException {
    	
    	long start=System.currentTimeMillis();
        String endpoint = args[0];
        BufferedWriter writer=new BufferedWriter(new FileWriter(args[1]));

        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefix("void",VOID2.NAMESPACE);
        
        Property property = model.createProperty(VOID2.property.toString());
        Property triples = model.createProperty(VOID2.triples.toString());
        Property distinctSubjects = model.createProperty(VOID2.distinctSubjects.toString());
        Property distinctObjects = model.createProperty(VOID2.distinctObjects.toString());
        Property entities = model.createProperty(VOID2.entities.toString());
         Property classProp = model.createProperty(VOID2.clazz.toString());
        //  Property triples  =model.createProperty(VOID2.triples.toString());

          
        Resource dataset = model.createResource();
        dataset.addProperty(RDF.type, model.createResource(VOID2.Dataset.toString()));
        dataset.addProperty(model.createProperty(VOID2.sparqlEndpoint.toString()), model.createResource(endpoint));
        
      
   QueryExecution    qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(tripleQuery));
    ResultSet rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            RDFNode my=sln.get("?triples");
           
            dataset.addProperty(triples, sln.get("?triples"));
  //          dataset.addProperty(triples, model.createLiteral(sln.get("?triples").toString()));
        }
   
   qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(distEntiryQuery));
   rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            dataset.addProperty(entities, sln.get("?entities"));
        }
        qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(distSubjQuery));
        rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            dataset.addProperty(distinctSubjects, sln.get("?subjects"));
        }
         qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(distObjQuery));
          rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            dataset.addProperty(distinctObjects, sln.get("?objects"));
        }
          qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(distPropQuery));
          rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            dataset.addProperty(model.createProperty(VOID2.properties.toString()), sln.get("?properties"));
        }  
       
         //  System.out.println(VOID2.Dataset);
        // model.write(System.out,);
         qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(propertyPartionQuery));
        //     System.out.println(generatedQuery);
        if (qexec == null) {
            return;
        }
       
         rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();

            QuerySolution sln = rs.nextSolution();
            Resource myProperty = sln.get("?property").asResource();
            RDFNode myTriples = sln.get("?subject_count");
            RDFNode distSubjects = sln.get("?subject_distinct_count");
            RDFNode distObjects = sln.get("?object_distinct_count");
        
            propertyObject.addProperty(property, myProperty);
            propertyObject.addProperty(triples, myTriples);
            propertyObject.addProperty(distinctSubjects,distSubjects);
            propertyObject.addProperty(distinctObjects, distObjects);
            dataset.addProperty(model.createProperty(VOID2.propertyPartition.toString()), propertyObject);
           
        
          
        }
       
        qexec = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(classPartionQuery));
        
        if (qexec == null) {
            return;
        }
   
         rs = qexec.execSelect();
        for (; rs.hasNext();) {
            Resource propertyObject = model.createResource();
            QuerySolution sln = rs.nextSolution();
            
            Resource type = sln.get("?type").asResource();
            RDFNode distEntity = sln.get("?distinctCount");
            
            propertyObject.addProperty(classProp, type);
            propertyObject.addProperty(entities, distEntity);
            dataset.addProperty(model.createProperty(VOID2.classPartition.toString()), propertyObject);
        }

        model.write(writer, "N3");
        
        long period=System.currentTimeMillis()-start;
        System.out.print("The metadata was acuired in(milSec): ");
		System.out.println(period);
     
    }

}
