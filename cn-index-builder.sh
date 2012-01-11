#!/bin/bash

# DataONE project builder utility.
# Handles building a project and projects its dependant on, for a 
# clean build-out.
#
# mvn clean, install is called in each project in PROJECTS array.
# Projects cleaned,installed in order of array, so order by dependency.
#
# jars in BUILDOUT_JARS are then copied to the BUILDOUT_DIR.
#
# Assumes buildout directory is rooted inside workspace directory.
# project_dir is a convienience variable assuming each project is 
# rooted from a common directory inside the workspace dir.
#
#  workspace_dir
#   -> project_dir (indexer)
#      -> project1,2,3,etc (java_common, libclient, etc.)
#   -> buildout_dir (root directory of the buildout project)
#
# Leave project dir blank if projects and buildout all located at same
# level under workspace_dir

WORKSPACE_DIR=/Users/sroseboo/development/workspace/

## path from workspace_dir to root of projects
PROJECT_DIR=indexer/
PROJECTS=(d1_common_java d1_libclient_java d1_cn_common d1_cn_noderegistry indexerapi d1_cn_index_common d1_cn_index_generator d1_cn_index_processor d1_cn_index_tool)
NUM_PROJECTS=${#PROJECTS[@]}

for ((i=0;i<$NUM_PROJECTS;i++)); do
	PROJECT=${PROJECTS[${i}]}
	echo ${PROJECT};
	cd ${WORKSPACE_DIR}${PROJECT_DIR}${PROJECT}
	mvn clean
	mvn -Dmaven.test.skip=true install
done

## buildout directory location - destination for build jars
## rooted from workspace_dir
BUILDOUT_DIR=cnBuildOut/cn-buildout/dataone-cn-index/usr/share/dataone-cn-index/

## Buildout jars directory path - rooted from project_dir
BUILDOUT_JARS=(d1_cn_index_generator/target/d1_index_task_generator_daemon.jar 
d1_cn_index_processor/target/d1_index_task_processor_daemon.jar d1_cn_index_tool/target/d1_index_build_tool.jar)
NUM_JARS=${#BUILDOUT_JARS[@]}

for ((i=0;i<$NUM_JARS;i++)); do
	JAR=${BUILDOUT_JARS[${i}]}
	echo ${JAR}
	cp ${WORKSPACE_DIR}${PROJECT_DIR}${JAR} ${WORKSPACE_DIR}${BUILDOUT_DIR}
done

### add input options to control:
# 1. build and copy jars, copy so
# 2. copy index config: solr schema, solr index field configuration to build?

