#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
  cat > /opt/run_test.sh <<-EOF
		base_path=\${1}
		# source /opt/gcc_env.sh
		cd \${base_path}/gpdb_src/gpMgmt/go-utils/src/gp_upgrade
		export GOPATH=\${base_path}/gpdb_src/gpMgmt/go-utils
		export PATH=\$PATH:/usr/local/go/bin:\$GOPATH/bin
		make dependencies

		# We need this to be after the make dependencies in order to
		# be able to do a go get. Currently, if you source
		# greenplum_path.sh and then run a git clone, you'll hit an
		# issue 'exit 128'
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		source \${base_path}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		make test
	EOF

	chmod a+x /opt/run_test.sh
}

function setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
}

function _main() {

    configure
    install_gpdb
    setup_gpadmin_user
    (make_cluster)
    gen_env
    run_test
}

_main "$@"
