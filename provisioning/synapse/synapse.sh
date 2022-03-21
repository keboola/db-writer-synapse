#!/usr/bin/env bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Required environment variables
: "${SYNAPSE_SERVER_NAME:?Need to set SYNAPSE_SERVER_NAME env variable}"
: "${AZURE_RESOURCE_GROUP:?Need to set AZURE_RESOURCE_GROUP env variable}"
: "${AZURE_SERVICE_PRINCIPAL_TENANT:?Need to set AZURE_SERVICE_PRINCIPAL_TENANT env variable}"
: "${AZURE_SERVICE_PRINCIPAL:?Need to set AZURE_SERVICE_PRINCIPAL env variable}"
: "${AZURE_SERVICE_PRINCIPAL_PASSWORD:?Need to set AZURE_SERVICE_PRINCIPAL_PASSWORD env variable}"

# EXPORTED ENV VARS BY CREATE CMD
# SYNAPSE_UID
# SYNAPSE_PWD
# SYNAPSE_SQL_SERVER_NAME
# SYNAPSE_DW_SERVER_NAME
# SYNAPSE_SERVER
# SYNAPSE_DATABASE
# SYNAPSE_RESOURCE_ID

# DESC: Runs az cli cmd under principal login
runCliCmd(){
    docker run --volume $(pwd):/keboola quay.io/keboola/azure-cli  \
        sh -c "az login --service-principal -u $AZURE_SERVICE_PRINCIPAL -p $AZURE_SERVICE_PRINCIPAL_PASSWORD --tenant $AZURE_SERVICE_PRINCIPAL_TENANT >> /dev/null && $1"
}

createServer(){
    export SYNAPSE_SERVER_PASSWORD=`openssl rand -base64 32`
    export DEPLOYMENT_NAME=${SYNAPSE_SERVER_NAME}"_"`openssl rand -hex 5`

    local output=$(runCliCmd "az group deployment create \
  --name ${DEPLOYMENT_NAME} \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --template-file /keboola/provisioning/synapse/synapse.json \
  --output json \
  --parameters \
    administratorLogin=keboola \
    administratorPassword=${SYNAPSE_SERVER_PASSWORD} \
    warehouseName=${SYNAPSE_SERVER_NAME} \
    warehouseCapacity=900")
    if [ $? -ne 0 ]; then
      echo "Deploy failed." 1>&2
      exit 1
    fi

    export SYNAPSE_SQL_SERVER_NAME=$(echo ${output} | jq -r '.properties.outputs.sqlServerName.value')
    export SYNAPSE_DW_SERVER_NAME=$(echo ${output} | jq -r '.properties.outputs.warehouseName.value')
    export SYNAPSE_RESOURCE_ID=$(echo ${output} | jq -r '.properties.outputs.warehouseResourceId.value')

    # Log messages to STDERR, so STDOUT contains only exports
    echo "Server deployed: $SYNAPSE_SQL_SERVER_NAME" 1>&2;

    local output=$(runCliCmd "az sql server firewall-rule create \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --server ${SYNAPSE_SQL_SERVER_NAME} \
  --name all \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 255.255.255.255")
    if [ $? -ne 0 ]; then
      echo "Firewall rule modification failed." 1>&2
      exit 1
    fi

    echo "Firewall rule set." 1>&2;
    echo "$output" 1>&2;

    # Print vars for php app
    echo "export SYNAPSE_UID=keboola"
    echo "export SYNAPSE_PWD=${SYNAPSE_SERVER_PASSWORD}"
    echo "export SYNAPSE_SQL_SERVER_NAME=${SYNAPSE_SQL_SERVER_NAME}"
    echo "export SYNAPSE_DW_SERVER_NAME=${SYNAPSE_DW_SERVER_NAME}"
    echo "export SYNAPSE_SERVER=${SYNAPSE_SQL_SERVER_NAME}.database.windows.net"
    echo "export SYNAPSE_DATABASE=${SYNAPSE_DW_SERVER_NAME}"
    echo "export SYNAPSE_RESOURCE_ID=${SYNAPSE_RESOURCE_ID}"
}

deleteServer(){
  : "${SYNAPSE_DW_SERVER_NAME:?Need to set SYNAPSE_DW_SERVER_NAME env variable (exported by the create cmd).}"
  : "${SYNAPSE_SQL_SERVER_NAME:?Need to set SYNAPSE_SQL_SERVER_NAME env variable (exported by the create cmd).}"
    local output=$(runCliCmd "az sql dw delete -y \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --name ${SYNAPSE_DW_SERVER_NAME} \
  --server ${SYNAPSE_SQL_SERVER_NAME}")

  echo "Synapse deleted." 1>&2;
  echo $output

      local output=$(runCliCmd "az sql server delete -y \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --name ${SYNAPSE_SQL_SERVER_NAME}")

  echo "Logical SQL server deleted." 1>&2;
  echo $output

# no right for principal to delete deploy
# https://github.com/keboola/php-table-backend-utils/pull/1#discussion_r403919848
#        local output=$(runCliCmd "az deployment delete \
#  --name ${DEPLOYMENT_NAME}")
#
#  echo "Deployment deleted."
#  echo $output
}


# DESC: Usage help
function script_usage() {
    cat << EOF
Usage
synapse.sh [-c| -d| -h]

Script for starting azure synapse.

 Options:
  -h|--help                Print this
  -c|--create              Create server
  -d|--delete              Delete server

To auto-export ENV variables, after creating the server, run:
SYNAPSE_ENV=`./provisioning/synapse/synapse.sh -c` && export $(echo ${SYNAPSE_ENV} | xargs)
EOF
}


while [[ $# -gt 0 ]]; do
    param="$1"
    shift
    case $param in
        -h | --help)
            script_usage
            ;;
        -c )
            createServer
            ;;
        -d )
            deleteServer
            ;;
        *)
            script_usage
            ;;
    esac
done
