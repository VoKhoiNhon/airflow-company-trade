#!/bin/bash

# Define color codes
RED=$(tput setaf 1)  # Set text color to red
BLUE=$(tput setaf 4)  # Set text color to blue
NORMAL=$(tput sgr0)   # Reset text color to normal

import_dags_components() {

	bash_cmds=(
		"ls -la dags/variables*.json | awk \"{print \\\$9}\" | xargs -I {} airflow variables import {}"
		"ls -la dags/connections*.json | awk \"{print \\\$9}\" | xargs -I {} airflow connections import {}"
	)

	for bash_cmd in "${bash_cmds[@]}"
	do
		entrypoint_docker_cmd="docker exec $(docker ps | grep 'airflow-scheduler' | awk '{print $1}')"
		execute_docker_cmd="$entrypoint_docker_cmd sh -c '$bash_cmd'"
		echo "${BLUE} --LOG: Execute CMD: $execute_docker_cmd ${NORMAL}"
		# Execute the command and handle errors
		if ! eval $execute_docker_cmd; then
			echo "${RED} --ERROR: Command failed - $bash_cmd ${NORMAL}"
			exit 1
		fi
	done
}

valid_dags() {
	entrypoint_docker_cmd="docker exec $(docker ps | grep 'airflow-scheduler' | awk '{print $1}')"
	count_error_dags_cmd="$entrypoint_docker_cmd sh -c \"airflow dags reserialize | grep \"ERROR\" | wc -l \""
	count_error_dags=$(eval $count_error_dags_cmd)
	echo "number of error:= $count_error_dags"
	if [ "$count_error_dags" -gt 0 ]; then
		echo "${BLUE} --LOG: Execute CMD: $count_error_dags_cmd ${NORMAL}"
		echo "${RED} --ERROR: DAGs Error: Has $count_error_dags error DAGs ${NORMAL}"
		show_error_dags_cmd="$entrypoint_docker_cmd sh -c \"airflow dags list-import-errors\""
		eval $show_error_dags_cmd
		exit 1
	else
		echo "${BLUE} --LOG: No Error DAGs ${NORMAL}"
		show_dags_cmd="$entrypoint_docker_cmd sh -c \"airflow dags list\""
		eval $show_dags_cmd
		exit 0
	fi;
}

main() {
	import_dags_components
	valid_dags
}

main
