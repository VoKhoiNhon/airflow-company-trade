#!/bin/bash

# Define color codes
RED=$(tput setaf 1)
BLUE=$(tput setaf 4)
NORMAL=$(tput sgr0)

# Default Variables
abs_filepath=$(readlink -f $0)
abs_dirpath=$(dirname $abs_filepath)
project_dirpath=$(dirname $abs_dirpath)
echo "${BLUE}LOG: Project Working Dir: $project_dirpath ${NORMAL}"

#####################################
# Generate Template Environment Files
#####################################
gen_temp_env_file() {
cat <<EOF > "$project_dirpath/.env"
# This is the project of Airflow which contains file config.
# Default: "."
AIRFLOW_ROOT_DIR=.
# This is directory of user home which is contains SSH-Keys to use SSHOperator.
# Default: "\$HOME"
USER_HOME_DIR=$HOME
AIRFLOW_IMAGE=apache/airflow:2.10.3-python3.10-custom
EOF
  
  echo "${BLUE}LOG: Preview file $project_dirpath/.env ${NORMAL}"
  cat $project_dirpath/.env
}
gen_extra_hosts() {
    echo "${BLUE}LOG: Generate extra_hosts file ${NORMAL}"
    cat /etc/hosts > $project_dirpath/extra_hosts.txt 
    cat $project_dirpath/extra_hosts.txt 
}
main() {
    gen_temp_env_file
    gen_extra_hosts
}
main
