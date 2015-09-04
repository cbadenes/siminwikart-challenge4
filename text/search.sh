#!/bin/bash
export MAVEN_OPTS="-Xmx12g"
mvn exec:java -Dexec.mainClass="es.upm.oeg.siminwikart.SearchArticles" -Dexec.classpathScope="test"
