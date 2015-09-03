/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edbt;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dgarijo
 */
public class EDBT {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String inputListOfNodes = "C:\\Users\\dgarijo\\Desktop\\EDBT SummerSchool\\proyecto\\finalNodes.csv";
        String categoryFile = "C:\\Users\\dgarijo\\Desktop\\EDBT SummerSchool\\proyecto\\article_category.csv";
        // Input: lista de ids (4462). Read it
        HashMap<String, ArrayList<String>> articleAndCategories = new HashMap<>();
        String line;
        String[] articleAndCat;
        try(BufferedReader br = new BufferedReader(new FileReader(inputListOfNodes))) {
            //StringBuilder sb = new StringBuilder();
            line = br.readLine();
            while (line != null) {
                line = line.replace("\"", "");
                articleAndCategories.put(line, new ArrayList<>());

                line = br.readLine();
            }
            System.out.println("Created hashMap with "+articleAndCategories.size()+ " components");
        } catch (Exception ex) {
            System.out.println("Error: "+ex.getMessage());
            ex.printStackTrace();
        }
        // Read the other stuff
        try(BufferedReader br = new BufferedReader(new FileReader(categoryFile))) {
            //StringBuilder sb = new StringBuilder();
            line = br.readLine();
           
            while (line != null) {
                String articleId = "",categoryId="";
                try{
                    articleAndCat = (line.replace("\"", "")).split(",");
    //                    System.out.println(line);
                    articleId = articleAndCat[0];
                    categoryId = articleAndCat[1];
                }catch(Exception e){
                    System.err.println("Error on "+ line);
                }
                if(articleAndCategories.containsKey(articleId)){
                    ArrayList<String> cats = articleAndCategories.get(articleId);
                    cats.add(categoryId);
                    //System.out.println(articleId+" has as category "+categoryId);
                }
                line = br.readLine();
            }
            
        } catch (Exception ex) {
            System.out.println("Error: "+ex.getMessage());
            //ex.printStackTrace();
        }
        
        
        //now compare one by one the entries and print the common categories.
        System.out.println("id article1, id article2, weight, common categories");
        Iterator<String> it = articleAndCategories.keySet().iterator();
        while(it.hasNext()){
            String article1 = it.next();
            ArrayList<String> catArticle1 = articleAndCategories.get(article1);
//            System.out.print(article1+":");
//            catArticle1.stream().forEach((a) -> {
//                System.out.print(a+",");
//            });
//            System.out.println();
            Iterator<String> it2 = articleAndCategories.keySet().iterator();
            while(it2.hasNext()){
                String article2 = it2.next();
                ArrayList<String> catArticle2 = articleAndCategories.get(article2);
                int commonCategories = 0;
                //if two categories are the same, increment commonCategoris
                commonCategories = catArticle1.stream().filter((c) -> (catArticle2.contains(c))).map((_item) -> 1).reduce(commonCategories, Integer::sum);
                if(commonCategories>0){
                    //weight = intersection/(categoriesofA+CategoriesOfB/2)
                    double media = (catArticle1.size()+catArticle2.size())/2;
                    double weight = commonCategories/media;
                    System.out.println(article1+","+article2+","+weight+","+commonCategories);
                }
            }
        }
        
    }
    
}
