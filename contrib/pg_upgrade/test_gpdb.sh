#!/bin/bash

# contrib/pg_upgrade/test_gpdb.sh
#
# Test driver for upgrading a Greenplum cluster with pg_upgrade. For test data,
# this script assumes the gpdemo cluster in gpAux/gpdemo/datadirs contains the
# end-state of an ICW test run. Performs a pg_dumpall, initializes a parallel
# gpdemo cluster and upgrades it against the ICW cluster and then performs
# another pg_dumpall. If the two dumps match then the upgrade created a new
# identical copy of the cluster.

unset PGHOST

OLD_BINDIR=/usr/local/gpdb/bin
OLD_DATADIR=$HOME/workspace/gpdb/gpAux/gpdemo/datadirs/
NEW_BINDIR=/usr/local/gpdb/bin
NEW_DATADIR=

qddir=

# The normal ICW run has a gpcheckcat call, so allow this testrunner to skip
# running it in case it was just executed to save time.
gpcheckcat=0

# Smoketesting pg_upgrade is done by just upgrading the QD without diffing the
# results. This is *NOT* a test of whether pg_upgrade can successfully upgrade
# a cluster but a test intended to catch when objects aren't properly handled
# in pg_dump/pg_upgrade wrt Oid synchronization
smoketest=0

# Not all platforms have a realpath binary in PATH, most notably macOS doesn't,
# so provide an alternative implementation. Returns an absolute path in the
# variable reference passed as the first parameter.  Code inspired by:
# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
realpath()
{
	local __ret=$1
	local path

	if [[ $2 = /* ]]; then
		path="$2"
	else
		path="$PWD/${2#./}"
	fi

	eval $__ret="'$path'"
}

restore_cluster()
{
	# Reset the pg_control files from the old cluster which were renamed
	# .old by pg_upgrade to avoid booting up an upgraded cluster.
	find ${OLD_DATADIR} -type f -name 'pg_control.old' |
	while read control_file; do
		mv "${control_file}" "${control_file%.old}"
	done

	# Remove the copied lalshell unless we're running in the gpdemo
	# directory where it's version controlled
	if ! git ls-files lalshell --error-unmatch >/dev/null 2>&1; then
		rm -f lalshell
	fi
}

upgrade_qd()
{
	mkdir -p $1

	# Run pg_upgrade
	pushd $1
	time ${NEW_BINDIR}/pg_upgrade --old-bindir=${OLD_BINDIR} --old-datadir=$2 --new-bindir=${NEW_BINDIR} --new-datadir=$3 --dispatcher-mode --progress
	if (( $? )) ; then
		echo "ERROR: Failure encountered in upgrading qd node"
		exit 1
	fi
	popd

	# Remember where we were when we upgraded the QD node. pg_upgrade generates
	# some files there that we need to copy to QE nodes.
	qddir=$1
}

gp_upgrade_new_cluster_preparation()
{
  sleep 10 #making sure it's up
  gp_upgrade prepare init-cluster --port $MASTER_DEMO_PORT #The port flag will get removed later on
}

upgrade_qd_with_gp_upgrade()
{
  echo "Printing out the command"
  echo "gp_upgrade upgrade convert-master --old-bindir=${OLD_BINDIR} --new-bindir=${NEW_BINDIR}" #These flags shouldn't exist later on
  gp_upgrade upgrade convert-master --old-bindir=${OLD_BINDIR} --new-bindir=${NEW_BINDIR} #These flags shouldn't exist later on
}

wait_for_qd_upgrade_to_finish()
{
  # Capture status upgrade log
  # Parse and see if the status upgrade is complete
    is_pg_upgrade_on_master_complete=false
  while ! $is_pg_upgrade_on_master_complete; do
    status_output=$(gp_upgrade status upgrade)
    res="COMPLETE - Run pg_upgrade on master"
    if  [[ "${status_output}" =~ "${res}" ]]; then
      echo "pg_upgrade on master done"
      echo "$status_output"
      is_pg_upgrade_on_master_complete=true

      # Current quick hack to make sure that the files do exist
      # XXX: How do we know that we've generated all the sql files?
      ls $gp_upgrade_dir/pg_upgrade_dump_*oids.sql
      if [ $? != 0 ]; then
        is_pg_upgrade_on_master_complete=false
      fi

    else
      echo "$status_output"
    fi

    sleep 0.5
  done

  # TODO: This will need to get eventually removed.
  # It looks like our `gp_upgrade status upgrade` is not reporting correctly.
  # We say that it's complete even though the pg_upgrade is still doing some
  # kind of verification.
  #sleep 10
}

wait_stop_clusters()
{
# There should be something here that ensures that the stop command finished
# This is where we should be checking the `gp_upgrade status upgrade to see if that stop finished.
  # Capture status upgrade log
  # Parse and see if the status upgrade is complete
    is_shutdown_clusters_complete=false
  while ! $is_shutdown_clusters_complete; do
    status_output=$(gp_upgrade status upgrade)
    res="COMPLETE - Shutdown clusters"
    if  [[ "${status_output}" =~ "${res}" ]]; then
      echo "gpstop for both clusters done"
      echo "$status_output"
      is_shutdown_clusters_complete=true

    else
      echo "$status_output"
    fi

    sleep 0.5
  done

}

upgrade_segment()
{
	mkdir -p $1

	# Copy the OID files from the QD to segments.
	cp $gp_upgrade_dir/pg_upgrade_dump_*_oids.sql $1

	# Run pg_upgrade
	pushd $1
	time ${NEW_BINDIR}/pg_upgrade --old-bindir=${OLD_BINDIR} --old-datadir=$2 --new-bindir=${NEW_BINDIR} --new-datadir=$3
	if (( $? )) ; then
		echo "ERROR: Failure encountered in upgrading node"
		exit 1
	fi
	popd
}

usage()
{
	appname=`basename $0`
	echo "$appname usage:"
	echo " -o <dir>     Directory containing old datadir"
	echo " -b <dir>     Directory containing binaries"
	echo " -s           Run smoketest only"
	echo " -C           Skip gpcheckcat test"
	exit 0
}

# Main
temp_root=`pwd`/tmp_check

while getopts ":o:b:sC" opt; do
	case ${opt} in
		o )
			realpath OLD_DATADIR "${OPTARG}"
			;;
		b )
			realpath NEW_BINDIR "${OPTARG}"
			realpath OLD_BINDIR "${OPTARG}"
			;;
		s )
			smoketest=1
			;;
		C )
			gpcheckcat=0
			;;
		* )
			usage
			;;
	esac
done

if [ -z "${OLD_DATADIR}" ] || [ -z "${NEW_BINDIR}" ]; then
	usage
fi

rm -rf "$temp_root"
mkdir -p "$temp_root"
if [ ! -d "$temp_root" ]; then
	echo "ERROR: unable to create workdir: $temp_root"
	exit 1
fi

trap restore_cluster EXIT

# The cluster should be running by now, but in case it isn't, issue a restart.
# Worst case we powercycle once for no reason, but it's better than failing
# due to not having a cluster to work with.
gpstart -a

# Run any pre-upgrade tasks to prep the cluster
if [ -f "test_gpdb_pre.sql" ]; then
  createdb regression
	psql -f test_gpdb_pre.sql regression
fi

# Ensure that the catalog is sane before attempting an upgrade. While there is
# (limited) catalog checking inside pg_upgrade, it won't catch all issues, and
# upgrading a faulty catalog won't work.
if (( $gpcheckcat )) ; then
	gpcheckcat
		if (( $? )) ; then
		echo "ERROR: gpcheckcat reported catalog issues, fix before upgrading"
		exit 1
	fi
fi

if (( !$smoketest )) ; then
	${NEW_BINDIR}/pg_dumpall --schema-only -f "$temp_root/dump1.sql"
fi

# gp_upgrade needs the older cluster up
echo "cleaning up the upgrade directory to prepare for a new full run"
rm -rf ~/.gp_upgrade
pkill gp_upgrade_hub
gp_upgrade prepare start-hub
gp_upgrade check config

# Create a new gpdemo cluster in the temproot. Using the old datadir for the
# path to demo_cluster.sh is a bit of a hack, but since this test relies on
# gpdemo having been used for ICW it will do for now.
export MASTER_DEMO_PORT=17432
export DEMO_PORT_BASE=27432
export NUM_PRIMARY_MIRROR_PAIRS=3
export MASTER_DATADIR=${temp_root}
cp ${OLD_DATADIR}/../lalshell .
BLDWRAP_POSTGRES_CONF_ADDONS=fsync=off ${OLD_DATADIR}/../demo_cluster.sh

NEW_DATADIR="${temp_root}/datadirs"

export MASTER_DATA_DIRECTORY="${NEW_DATADIR}/qddir/demoDataDir-1"
export PGPORT=17432

gp_upgrade_new_cluster_preparation

stop_command="gp_upgrade prepare shutdown-clusters --old-bindir=$OLD_BINDIR --new-bindir=$NEW_BINDIR"
echo "STOP COMMAND: $stop_command"
$stop_command
wait_stop_clusters

MASTER_DATA_DIRECTORY=""; unset MASTER_DATA_DIRECTORY
PGPORT=""; unset PGPORT
PGOPTIONS=""; unset PGOPTIONS

# Reset the hub without any environment
pkill gp_upgrade_hub
gp_upgrade prepare start-hub

# Start by upgrading the master
#upgrade_qd "${temp_root}/upgrade/qd" "${OLD_DATADIR}/qddir/demoDataDir-1/" "${NEW_DATADIR}/qddir/demoDataDir-1/"
gp_upgrade_dir="${temp_root}/upgrade/qd"

qddir="${temp_root}/upgrade/qd"
qddatadir="${NEW_DATADIR}/qddir/demoDataDir-1"
gp_upgrade_dir=${HOME}/.gp_upgrade/pg_upgrade
upgrade_qd_with_gp_upgrade "${OLD_DATADIR}/qddir/demoDataDir-1/" "${NEW_DATADIR}/qddir/demoDataDir-1/"
wait_for_qd_upgrade_to_finish

# If this is a minimal smoketest to ensure that we are pulling the Oids across
# from the old cluster to the new, then exit here as we have now successfully
# upgraded a node (the QD).
if (( $smoketest )) ; then
	restore_cluster
	exit
fi

# Upgrade all the segments and mirrors. In a production setup the segments
# would be upgraded first and then the mirrors once the segments are verified.
# In this scenario we can cut corners since we don't have any important data
# in the test cluster and we only consern ourselves with 100% success rate.
#for i in 1 2 3
#do
#	j=$(($i-1))
#	upgrade_segment "${temp_root}/upgrade/dbfast$i" "${OLD_DATADIR}/dbfast$i/demoDataDir$j/" "${NEW_DATADIR}/dbfast$i/demoDataDir$j/"
#	upgrade_segment "${temp_root}/upgrade/dbfast_mirror$i" "${OLD_DATADIR}/dbfast_mirror$i/demoDataDir$j/" "${NEW_DATADIR}/dbfast_mirror$i/demoDataDir$j/"
#done

#. ${NEW_BINDIR}/../greenplum_path.sh

# Start the new cluster, dump it and stop it again when done. We need to bump
# the exports to the new cluster for starting it but reset back to the old
# when done. Set the same variables as gpdemo-env.sh exports. Since creation
# of that file can collide between the gpdemo clusters, perform it manually
#export PGPORT=17432
#export MASTER_DATA_DIRECTORY="${NEW_DATADIR}/qddir/demoDataDir-1"
#gpstart -a
#
## Run any post-upgrade tasks to prep the cluster for diffing
#if [ -f "test_gpdb_post.sql" ]; then
#	psql -f test_gpdb_post.sql regression
#fi

#${NEW_BINDIR}/pg_dumpall --schema-only -f "$temp_root/dump2.sql"
#gpstop -a
#export PGPORT=15432
#export MASTER_DATA_DIRECTORY="${OLD_DATADIR}/qddir/demoDataDir-1"

# Since we've used the same pg_dumpall binary to create both dumps, whitespace
# shouldn't be a cause of difference in the files but it is. Partitioning info
# is generated via backend functionality in the cluster being dumped, and not
# in pg_dump, so whitespace changes can trip up the diff.
#if diff -w "$temp_root/dump1.sql" "$temp_root/dump2.sql" >/dev/null; then
#	echo "Passed"
#	exit 0
#else
	# To aid debugging in pipelines, print the diff to stdout
#	diff "$temp_root/dump1.sql" "$temp_root/dump2.sql"
#	echo "Error: before and after dumps differ"
#	exit 1
#fi
