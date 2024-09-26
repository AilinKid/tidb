#! /usr/bin/env bash

# help
# download some third party tools for br&lightning integration test
# example: ./download_integration_test_binaries.sh master


set -o errexit
set -o pipefail


# Specify which branch to be utilized for executing the test, which is
# exclusively accessible when obtaining binaries from
# http://fileserver.pingcap.net.
branch=${1:-master}
file_server_url=${2:-http://fileserver.pingcap.net}

tikv_importer_branch="release-5.0"
tikv_sha1_url="${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1"
pd_sha1_url="${file_server_url}/download/refs/pingcap/pd/${branch}/sha1"
tiflash_sha1_url="${file_server_url}/download/refs/pingcap/tiflash/${branch}/sha1"
ticdc_sha1_url="${file_server_url}/download/refs/pingcap/ticdc/${branch}/sha1"
tikv_importer_sha1_url="${file_server_url}/download/refs/pingcap/importer/${tikv_importer_branch}/sha1"

pd_sha1=$(curl "$pd_sha1_url")
tikv_sha1=$(curl "$tikv_sha1_url")
tiflash_sha1=$(curl "$tiflash_sha1_url")
tikv_importer_sha1=$(curl "$tikv_importer_sha1_url")
ticdc_sha1=$(curl "$ticdc_sha1_url")

# download pd / tikv / tiflash binary build from tibuid multibranch pipeline
pd_download_url="${file_server_url}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
tiflash_download_url="${file_server_url}/download/builds/pingcap/tiflash/${branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"
tikv_importer_download_url="${file_server_url}/download/builds/pingcap/importer/${tikv_importer_sha1}/centos7/importer.tar.gz"
ticdc_download_url="${file_server_url}/download/builds/pingcap/ticdc/${ticdc_sha1}/centos7/ticdc-linux-amd64.tar.gz"


# download some dependencies tool binary from file server
minio_url="${file_server_url}/download/builds/minio/minio/RELEASE.2020-02-27T00-23-05Z/minio"
go_ycsb_url="${file_server_url}/download/builds/pingcap/go-ycsb/test-br/go-ycsb"
minio_cli_url="${file_server_url}/download/builds/minio/minio/RELEASE.2020-02-27T00-23-05Z/mc"
kes_url="${file_server_url}/download/kes"
fake_gcs_server_url="${file_server_url}/download/builds/fake-gcs-server"
brv_url="${file_server_url}/download/builds/brv4.0.8"
# already manually uploaded to file server for localstack
localstack_url="${file_server_url}/download/localstack-cli.tar.gz"

set -o nounset

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

function download() {
    local url=$1
    local file_name=$2
    local file_path=$3
    if [[ -f "${file_path}" ]]; then
        echo "file ${file_name} already exists, skip download"
        return
    fi
    echo "download ${file_name} from ${url}"
    wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

function main() { 
    rm -rf third_bin
    rm -rf tmp
    mkdir third_bin
    mkdir tmp
    
    #PD server
    download "$pd_download_url" "pd-server.tar.gz" "tmp/pd-server.tar.gz"
    tar -xzf tmp/pd-server.tar.gz -C third_bin --wildcards 'bin/*'
    mv third_bin/bin/* third_bin/ 

    #TiKV server
    download "$tikv_download_url" "tikv-server.tar.gz" "tmp/tikv-server.tar.gz"
    tar -xzf tmp/tikv-server.tar.gz -C third_bin --wildcards 'bin/*'
    mv third_bin/bin/* third_bin/

    #TiFlash
    download "$tiflash_download_url" "tiflash.tar.gz" "tmp/tiflash.tar.gz"
    tar -xzf tmp/tiflash.tar.gz -C third_bin
    mv third_bin/tiflash third_bin/_tiflash
    mv third_bin/_tiflash/* third_bin && rm -rf third_bin/_tiflash

    #TiKV Importer
    download "$tikv_importer_download_url" "importer.tar.gz" "tmp/importer.tar.gz"
    tar -xzf tmp/importer.tar.gz -C third_bin --wildcards 'bin/tikv-importer'
    mv third_bin/bin/tikv-importer third_bin/

    #TiCDC
    download "$ticdc_download_url" "ticdc-linux-amd64.tar.gz" "tmp/ticdc-linux-amd64.tar.gz"
    tar -xzf tmp/ticdc-linux-amd64.tar.gz -C third_bin --wildcards '*/bin/*'
    mv third_bin/ticdc-linux-amd64/bin/* third_bin/
    rm -rf third_bin/ticdc-linux-amd64

    download "$minio_url" "minio" "third_bin/minio"
    download "$go_ycsb_url" "go-ycsb" "third_bin/go-ycsb"
    download "$minio_cli_url" "mc" "third_bin/mc"
    download "$kes_url" "kes" "third_bin/kes"
    download "$fake_gcs_server_url" "fake-gcs-server" "third_bin/fake-gcs-server"
    download "$brv_url" "brv4.0.8" "third_bin/brv4.0.8"

    # Download and set up LocalStack
    download "$localstack_url" "localstack-cli.tar.gz" "tmp/localstack-cli.tar.gz"
    mkdir -p tmp/localstack_extract
    tar -xzf tmp/localstack-cli.tar.gz -C tmp/localstack_extract
    mv tmp/localstack_extract/localstack/* third_bin/
    rm -rf tmp/localstack_extract

    chmod +x third_bin/*
    rm -rf tmp
    rm -rf third_bin/bin
    ls -alh third_bin/
}

main "$@"

color-green "Download SUCCESS"
