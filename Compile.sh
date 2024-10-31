#!/bin/bash
hadoop com.sun.tools.javac.Main Accidents/*.java
jar cf Accidents.jar Accidents/*.class
hadoop fs -put Accidents.jar /CS435-Term-Project
hadoop fs -put Accidents /CS435-Term-Project