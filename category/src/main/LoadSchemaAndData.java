/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edbt;

import com.sparsity.sparksee.script.ScriptParser;
import java.io.IOException;

/**
 *
 * @author dgarijo
 */
public class LoadSchemaAndData {
    public static void main(String[] args) throws IOException{
        ScriptParser sp = new ScriptParser();

	long start = System.currentTimeMillis();
        //
        // TODO: Create schema by loading ./data/database/scripts/schema.des script
        //
        sp.parse("C:\\Users\\dgarijo\\Documents\\GitHub\\siminwikart-challenge4\\category\\sparksee\\schema6.des", true, "");
        //nodes
        sp.parse("C:\\Users\\dgarijo\\Documents\\GitHub\\siminwikart-challenge4\\category\\sparksee\\load6articles_ids6.des", true, "");
        sp.parse("C:\\Users\\dgarijo\\Documents\\GitHub\\siminwikart-challenge4\\category\\sparksee\\load6categories_ids6.des", true, "");
        //edges        
        sp.parse("C:\\Users\\dgarijo\\Documents\\GitHub\\siminwikart-challenge4\\category\\sparksee\\load6article_category6.des", true, "");
        sp.parse("C:\\Users\\dgarijo\\Documents\\GitHub\\siminwikart-challenge4\\category\\sparksee\\load6categories_relations6.des", true, "");
        
    }
    
}
