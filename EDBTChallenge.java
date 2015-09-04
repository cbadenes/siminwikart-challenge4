/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sparsity.sparksee.tutorial;

import com.sparsity.sparksee.gdb.Database;
import com.sparsity.sparksee.gdb.EdgesDirection;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Objects;
import com.sparsity.sparksee.gdb.ObjectsIterator;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Sparksee;
import com.sparsity.sparksee.gdb.SparkseeConfig;
import com.sparsity.sparksee.gdb.Value;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.apache.logging.log4j.core.Logger;
import sun.org.mozilla.javascript.TopLevel;
//import org.apache.log4j.PropertyConfigurator;
//import org.apache.log4j.Logger;
        
/**
 *
 * @author freddy,dani
 */
public class EDBTChallenge {
    private Session session = null;
    private Logger logger = null;
    
    String badCategoriesFileURL = "wikipedia_bad_categories.txt";
    private List<Long> badCategories = null;
    
    public EDBTChallenge(Session session) {
        this.session = session;
        //PropertyConfigurator.configure("log4j.properties");
        //this.logger = Logger.getLogger(this.getClass());
    }
    
    public static void main(String[] args)throws FileNotFoundException, Throwable{
        long[] articlesArray = {507433,240390,2484564,2150841};
        Set<Long> articlesSet = new HashSet<Long>();
        for(long articleID : articlesArray) {articlesSet.add(new Long(articleID));}
        
        final SparkseeConfig cfg = new SparkseeConfig();
	final Sparksee sparksee = new Sparksee(cfg);
	final Database db = sparksee.open("./data/wikipedia6.gdb", false);
	final Session session = db.newSession();
        //iker casillas: 507433. Fernando Alonso: 240390
        //Akiko: 
        
        final EDBTChallenge challenge = new EDBTChallenge(session);
        String badCategoriesFileURL = "wikipedia_bad_categories.txt";
        List<Long> badCategories = challenge.getBadCategories(badCategoriesFileURL);
        challenge.badCategories = badCategories;
        System.out.println("badCategories = " + badCategories);
        
//        final Set<Long> commonCategories = challenge.getCommonCategories(240390,240390);
//        System.out.println("commonCategories = " + commonCategories);
//        System.out.println("commonCategories.size = " + commonCategories.size());
        
        
        final Set<ArticlesSimilarity> articlesSimilarity = challenge.calculateSimilarity(articlesSet, 3);
        System.out.println("articlesSimilarity = " + articlesSimilarity);
        //System.out.println("articlesSimilarity.size = " + articlesSimilarity.size());
        
//        long[] articlesArray2 = {240390,240390};
//        Set<Long> articlesSet2 = new HashSet<Long>();
//        for(long articleID : articlesArray2) {articlesSet2.add(new Long(articleID));}
//        final Set<ArticlesSimilarity> articlesSimilarity2 = challenge.calculateSimilarity(articlesSet2, 1);
//        System.out.println("articlesSimilarity2 = " + articlesSimilarity2);
        
//          Set<Long> categories = challenge.getCategories(240390);
//          System.out.println("categories = " + categories);
//          System.out.println("categories.size = " + categories.size());
          
//        final Set<Long> commonCategories = challenge.getCommonCategories(240390,240390, 0);
//        System.out.println("commonCategories = " + commonCategories);
//        System.out.println("commonCategories.size = " + commonCategories.size());
//
//        final Set<Long> categories = challenge.getCategories(507433);
//        System.out.println("categories = " + categories);
//        System.out.println("categories.size = " + categories.size());
        
            //Set<Long> parentCategories = challenge.getParents(new Long(1005395), 1);
            //System.out.println("parentCategories = " + parentCategories);
//        double similarityValue = challenge.calculateSimilarity(507433, 240390, 1);
//        System.out.println("similarityValue = " + similarityValue);
        
    }


    private Set<Long> getCommonCategories(long article1, long article2, int depth) {
        final Set<Long> result = new HashSet<Long>();
        
        final Set<Long> commonCategories = this.getCommonCategories(article1,article2);
        result.addAll(commonCategories);
        
        final Map<Long, Set<Long>> mapCategoryParents = new HashMap<Long, Set<Long>>();
        
        for(Long commonCategory : commonCategories) {
            final Set<Long> categoryParents = this.getParents(commonCategory, depth);
            mapCategoryParents.put(commonCategory, categoryParents);
            result.addAll(categoryParents);
        }
        
        //System.out.println("mapCategoryParents = " + mapCategoryParents);
        return result;
    }
    
    private static Set<Long> objectsToSet(Objects objects) {
        final Set<Long> result = new HashSet<Long>();
        if(objects != null) {
            final ObjectsIterator it = objects.iterator();
            while(it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }
    
    private Set<Long> getCategories(long art1) {
        final Graph g = this.session.getGraph();
	final Value v = new Value();
        final int artType = g.findType("Article");
        final int artId = g.findAttribute(artType, "article_id");
        final long art1OID = g.findObject(artId, v.setLong(art1));
        final Objects categoriesObjects = g.neighbors(art1OID, g.findType("hasCategory"), EdgesDirection.Any);
        final Set<Long> categoriesSet = EDBTChallenge.objectsToSet(categoriesObjects);
//        System.out.println("categoriesSet before cleaning = " + categoriesSet);
//        System.out.println("this.badCategories = " + this.badCategories);
        int beforeCleaningSize = categoriesSet.size();
        categoriesSet.removeAll(this.badCategories);
        //System.out.println("categoriesSet after cleaning = " + categoriesSet);
        int afterCleaningSize = categoriesSet.size();
        int removedCategoriesSize = beforeCleaningSize - afterCleaningSize;
        //if(removedCategoriesSize > 0) {
            System.out.println("getCategories() article " + art1 + ", number of bad categories removed = " + removedCategoriesSize);
        //}
        
        
        
        return categoriesSet;
    }

    private Set<Long> getCategories(long art1, int depth) {
        Set<Long> result = new HashSet<Long>();
        
        Set<Long> categories = this.getCategories(art1);
        //System.out.println("categories = " + categories);
        result.addAll(categories);
        for(Long category : categories) {
            Set<Long> parents = this.getParents(category, depth);
            //System.out.println("parents = " + parents);
            result.addAll(parents);
        }
        //System.out.println("categories = " + categories);
        
        return result;
    }
    
    private Set<Long> getCommonCategories(long art1, long art2) {
        final Graph g = this.session.getGraph();
	final Value v = new Value();
        final int artType = g.findType("Article");
        final int artId = g.findAttribute(artType, "article_id");
        final long art1OID = g.findObject(artId, v.setLong(art1));
        final long art2OID = g.findObject(artId, v.setLong(art2));
        final Objects cats1 = g.neighbors(art1OID, g.findType("hasCategory"), EdgesDirection.Any);
//        System.out.println("cats1 = " + cats1);
//        System.out.println("cats1.size = " + cats1.size());

        final Objects cats2 = g.neighbors(art2OID, g.findType("hasCategory"), EdgesDirection.Any);
//        System.out.println("cats2 = " + cats2);
//        System.out.println("cats2.size = " + cats2.size());
        
        cats1.intersection(cats2);
        int commonCategoriesSizeBeforeCleaning = cats1.size();
        
        
        final Set<Long> commonCategories = EDBTChallenge.objectsToSet(cats1);
        commonCategories.removeAll(this.badCategories);
        int commonCategoriesSizeAfterCleaning = commonCategories.size();
        int removedBadCategoriesSize = commonCategoriesSizeBeforeCleaning - commonCategoriesSizeAfterCleaning;
        
        if(removedBadCategoriesSize > 0) {
            System.out.println(removedBadCategoriesSize + " bad categories removed from getCommonCategories()");
        }
        
        return commonCategories;
    }
    
    private Set<ArticlesSimilarity> calculateSimilarity(Set<Long> articles, int depth) {
        final Set<ArticlesSimilarity> result = new HashSet<ArticlesSimilarity>();
        List<Long> list1 = new ArrayList<Long>(articles);
        List<Long> list2 = new ArrayList<Long>(list1);
        for(Long aid1 : list1) {
            for(Long aid2 : list2) {
                Set<Long> commonCategories = this.getCommonCategories(aid1, aid2, depth);
                double similarityValue = this.calculateSimilarity(aid1, aid2, depth);
                
                ArticlesSimilarity articleSimilarity = new ArticlesSimilarity(
                        aid1, aid2, commonCategories, similarityValue);
                result.add(articleSimilarity);
            }
        }
        
        return result;
    }
    
    private double calculateSimilarity(long aid1, long aid2, int depth) {
        Set<Long> commonCategoriesWithParents = this.getCommonCategories(aid1, aid2, depth);
        System.out.println("\t\tcommonCategoriesWithParents = " + commonCategoriesWithParents);
        System.out.println("\t\tcommonCategoriesWithParents.size = " + commonCategoriesWithParents.size());
                
        Set<Long> categoriesWithParent1 = this.getCategories(aid1, depth);
        System.out.println("\t\tcategoriesWithParent1.size = " + categoriesWithParent1.size());
        
        Set<Long> categoriesWithParent2 = this.getCategories(aid2, depth);
        System.out.println("\t\tcategoriesWithParent2.size = " + categoriesWithParent2.size());
        
        //double similarityValue = commonCategoriesWithParents.size() / ((categoriesWithParent1.size() + categoriesWithParent2.size()) / 2);
        double arriba = commonCategoriesWithParents.size();
        double abajo = ((categoriesWithParent1.size() + categoriesWithParent2.size()) / 2);
        
        double similarityValue = arriba / abajo;
        //System.out.println("similarityValue.size = " + similarityValue);
        //if(similarityValue > 1) {
            System.out.println(aid1 + "," + aid2 + ":" + similarityValue);
        //}
        
        return similarityValue;
        
    }
    
    private List<Long> getBadCategories(String categoriesToFilter) {
        if(this.badCategories != null) {
            return this.badCategories;
        } else {
            List<Long> badCategories = new LinkedList<Long>();

            String line="";
            String[] articleAndCat={};
            try {
                BufferedReader br = new BufferedReader(new FileReader(categoriesToFilter));
                //StringBuilder sb = new StringBuilder();
                line = br.readLine();

                while (line != null) {
                    String categoryId="";
                    try{
                        articleAndCat = (line.replace("\"", "")).split(",");
        //                    System.out.println(line);
                        categoryId = articleAndCat[0];
                        //categoryId = articleAndCat[1];
                    }catch(Exception e){
                        System.err.println("Error on "+ line);
                    }

                    try {
                        badCategories.add(Long.parseLong(categoryId));
                    } catch (Exception e) {
                        //System.err.println("Error while parsing category id to Long : " + categoryId);
                    }

    //                if(articleAndCategories.containsKey(articleId)){
    //                    ArrayList<String> cats = articleAndCategories.get(articleId);
    //                    cats.add(categoryId);
    //                    //System.out.println(articleId+" has as category "+categoryId);
    //                }
                    line = br.readLine();
                }

            } catch (Exception ex) {
                System.out.println("Error: "+ex.getMessage());
                //ex.printStackTrace();
            }
            //System.out.println("Finished reading bad categories");
            
            List<Long> result = new LinkedList<Long>();
            Graph g = this.session.getGraph();
            for(Long badCategory : badCategories) {
                final int typeCategory = g.findType("Category");
                final int attrCategoryID = g.findAttribute(typeCategory, "category_id");
                final Value v = new Value();
                final long badCategoryOID = g.findObject(attrCategoryID, v.setLong(badCategory));                
                result.add(badCategoryOID);
            }
        
            return result;            
        }
    }
    
    private Set<Long> getDirectParent(Long categoryID) {
        //System.out.println("categoryID = " + categoryID);
        final Graph g = this.session.getGraph();
        final Objects directParentsObjects = g.neighbors(categoryID, g.findType("subclass"), EdgesDirection.Outgoing);
        final Set<Long> directParents = EDBTChallenge.objectsToSet(directParentsObjects);
        final int directParentsSizeBeforeCleaning = directParents.size();
//        System.out.println("directParents before cleaning = " + directParents);
//        System.out.println("this.badCategories = " + this.badCategories);
        directParents.removeAll(this.badCategories);
        final int directParentsSizeAfterCleaning = directParents.size();
        final int removedParentsSize = directParentsSizeBeforeCleaning - directParentsSizeAfterCleaning;
        if(removedParentsSize > 0) {
            System.out.println("category : " + categoryID + " , number of bad categories removed = " + removedParentsSize);
        }
        //System.out.println("directParents after cleaning = " + directParents);
        return directParents;
    }
    
    private Set<Long> getParents(Long categoryID, int depth) {
        if(depth <= 0) {
            return new HashSet<Long>();
        } else {
            final Set<Long> allParents = new HashSet<Long>();
            
            //get direct parents
            final Set<Long> directParents = this.getDirectParent(categoryID);
            //System.out.println("\tdirectParents.size in level " + depth + " , " + directParents.size());

            //and for each of the direct parents
            for(Long directParent : directParents) {
                //first, add the direct parents
                allParents.add(directParent);
            
                //then, add all the indirect parents, recursively
                final Set<Long> indirectParents = getParents(directParent, depth-1);
                //System.out.println("\tindirectParents.size in level " + depth + " , " + indirectParents.size());
                allParents.addAll(indirectParents);
            }
            //System.out.println("\tallParents.size in level " + depth + " , " + allParents.size());
//            System.out.println(intermediateObject.size());
            return allParents;
        }
    }
    
    class ArticlesSimilarity {
        Long article1;
        Long article2;
        Set<Long> commonCategories;
        double similarityValue = 0;
        
        ArticlesSimilarity(Long article1, Long article2, Set<Long> commonCategories, double similarityValue) {
            this.article1 = article1;
            this.article2 = article2;
            this.commonCategories = commonCategories;
            this.similarityValue = similarityValue;
        }
        
        public String toString() {
            String commonCategoriesString ="";
            for(long commonCategory : commonCategories) {
                commonCategoriesString += commonCategory + ",";
            }
            commonCategoriesString += "}";
            
            
            return "\n\narticle pair=(" + this.article1 + "," + this.article2 + ")" + ";\nweight=<" + this.similarityValue + ">;\ncommon categories=" + commonCategoriesString;
        }
    }
}

