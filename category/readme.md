Category based similarity
======
Script that takes as input a list of article ids:

"16281554"

"40408235"

"16732960"

And an article-category file like http://www.sparsity-technologies.com/downloads/WikipediaDump/article_category.tar.gz with two columns: article_id_from,category_id_to.

From these resources, a weighted graph is generated following the format: id article1, id article2, weight, common categories.

Where weight = commonCategories/((categories_from_article1+categories_from_article2)/2)

Example: 24461910,23689299,0.15384615384615385,2
