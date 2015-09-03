#!/bin/bash
export MAVEN_OPTS="-Xmx64g"
mvn exec:java -Dexec.mainClass="es.upm.oeg.siminwikart.CreateTopicModel" -Dexec.classpathScope="test"
