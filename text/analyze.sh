#!/bin/bash
export MAVEN_OPTS="-Xmx15g"
mvn exec:java -Dexec.mainClass="es.upm.oeg.siminwikart.CreateTopicModel" -Dexec.classpathScope="test"
