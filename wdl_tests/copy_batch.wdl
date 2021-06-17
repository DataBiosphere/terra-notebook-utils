version 1.0

workflow drs {
    meta {
        description: "This workflow tests downloading DRS URIs using terra-notebook-utils."
        tags: "DRS"
        authors: "M. Baumann, L. Blauvelt, B. Hannafious"
    }
    parameter_meta {
        drs_uris: "Array of DRS URIs to be downloaded"
    }
    input {
        Array[String] drs_uris
    }

    call create_manifest {
        input: drs_uris=drs_uris
    }
    call tnu_download {
        input:
            manifest=create_manifest.tnu_manifest
    }
}

task create_manifest {
    meta {
        description: "Creates a manifest mapping DRS URIs to local/gs schema appropriate for use with terra-notebook-utils."
    }
    parameter_meta {
        drs_uris: "Array of DRS URIs to be downloaded"
        cpu: "runtime parameter - number of CPUs "
        memory: "runtime parameter - amount of memory to allocate in GB. Default is: 16"
        boot_disk: "runtime parameter - amount of boot disk space to allocate in GB. Default is: 50"
        disk: "runtime parameter - amount of disk space to allocate in GB. Default is: 128"
    }
    input {
        Array[String] drs_uris
        Int? cpu
        Int? memory
        Int? boot_disk
        Int? disk

        String tnu_manifest_filename = "./manifest.json"
    }
    command <<<
        set -eux o pipefail

        # Identify the runtime environment; Check Linux version
        cat /etc/issue
        uname -a

        # Commands that could be added to the Dockerfile
        apt-get update
        apt-get install -yq --no-install-recommends apt-utils jq

        # Create tnu manifest
        echo '"~{sep='" "' drs_uris}"' | jq -s '[.[] as $item | {drs_uri: $item, dst: "."}]' > ~{tnu_manifest_filename}
    >>>

    output {
        File tnu_manifest = tnu_manifest_filename
    }

    runtime {
        docker: "broadinstitute/cromwell-drs-localizer:61"
        cpu: select_first([cpu, "4"])
        memory: select_first([memory,"16"]) + " GB"
        disks: "local-disk " + select_first([disk, "128"]) + " HDD"
        bootDiskSizeGb: select_first([boot_disk,"30"])
    }
}

task tnu_download {
    meta {
        description: "This task tests downloading DRS URIs using terra-notebook-utils."
    }
    parameter_meta {
        manifest: "Manifest mapping DRS URIs to local or gs destinations."
        cpu: "runtime parameter - number of CPUs"
        memory: "runtime parameter - amount of memory to allocate in GB. Default is: 16"
        boot_disk: "runtime parameter - amount of boot disk space to allocate in GB. Default is: 50"
        disk: "runtime parameter - amount of disk space to allocate in GB. Default is: 128"
    }
    input {
        File manifest
        Int? cpu
        Int? memory
        Int? boot_disk
        Int? disk
    }
    command <<<
        set -eux o pipefail

        CURRENT_DIR=$(pwd)

        # Where all non-tnu downloads go (wget, gsutil, and curl)
        # TMP_DL_DIR=/cromweLl_root/speedtest3crdws3s
        # mkdir -p ${TMP_DL_DIR}

        # Identify the runtime environment; Check Linux version
        cat /etc/issue
        uname -a

        # Commands that could be added to the Dockerfile
        apt-get update
        apt-get install -yq --no-install-recommends apt-utils git

        # Install terra-notebook-utils
        python -m pip install git+https://github.com/DataBiosphere/terra-notebook-utils
        python -m pip show terra-notebook-utils

        # Download the files in the manifest
        start_time=`date +%s`
        time tnu drs copy-batch --workspace terra-notebook-utils-tests --workspace-namespace firecloud-cgl --manifest ~{manifest}
        tnu_exit_status=$?
        echo "tnu exit status: "$tnu_exit_status
        end_time=`date +%s`
        total_time="$(($end_time-$start_time))"
        # Check final disk usage after downloading
        df -h

        # TODO: verify that the downloaded files exist
    >>>

    runtime {
        docker: "broadinstitute/cromwell-drs-localizer:61"
        cpu: select_first([cpu, "4"])
        memory: select_first([memory,"16"]) + " GB"
        disks: "local-disk " + select_first([disk, "128"]) + " HDD"
        bootDiskSizeGb: select_first([boot_disk,"30"])
    }
}
