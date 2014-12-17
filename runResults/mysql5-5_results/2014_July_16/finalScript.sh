#!/bin/bash
USERNAME=epattuk
USERPASS=
REMOTEHOST=lva1-app1585.prod.linkedin.com
MYSQLFOLDER=/mnt/u002/mysql
BACKUPFOLDER=/mnt/u002/mysqlBackup
WORKFOLDER=/mnt/u002/finalRun
PLANFOLDER=$WORKFOLDER/plans
RESULTFOLDER=$WORKFOLDER/results
MEMHOGFOLDER=$WORKFOLDER/memhog
CONFIGSFOLDER=$WORKFOLDER/configs5_5

EXPRUNTIME=3600
GRAN=10
CLIENTSWAIT=20
SAFETYWAITTIME=300
TOTALRUNTIME=0
let TOTALRUNTIME=CLIENTSWAIT+EXPRUNTIME+SAFETYWAITTIME
MYSQLRUNTIME=0
let MYSQLRUNTIME=CLIENTSWAIT+EXPRUNTIME


function mysqlStop {
	ssh $USERNAME@$REMOTEHOST "mysql -uperftool -pperftool -e 'purge binary logs before now();'"
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo /etc/init.d/mysql stop"
}

function mysqlStart {
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo taskset 0xfff000 /etc/init.d/mysql start"
}

function restoreDB {
	mysqlStop
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo rm -r $MYSQLFOLDER"
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo cp -rp $BACKUPFOLDER $MYSQLFOLDER"
	mysqlStart
}

function memhogLock {
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo $MEMHOGFOLDER/memhog -a $1" &
}

function memhogUnlock {
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo killall memhog"
}

function executeProxyExperiment {
	restoreDB
	memhogLock 42949672960
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; taskset 0x00000f java -Xmx1g -cp \"lib/*\" com.linkedin.multitenant.main.RunStatusChecker -host=localhost -port=3306 -user=perftool -pass=perftool -run=$TOTALRUNTIME -gran=$GRAN -out=$RESULTFOLDER/proxy$1-$2_$3_crazy_mysqlSC.html" &
	echo "***** started mysql status checker *****"	
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; taskset 0x00000f java -Xmx8g -cp \"lib/*\" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$RESULTFOLDER/proxy$1-$2_$3_crazy_perfgc.txt com.linkedin.multitenant.main.RunExperiment -plan=$PLANFOLDER/proxyPlan_$3_crazy.xml -wait=$CLIENTSWAIT" &
	echo "***** started clients *****"
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; taskset 0x000ff0 java -Xmx8g -cp \"lib/*\" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$RESULTFOLDER/proxy$1-$2_$3_crazy_proxygc.txt com.linkedin.proxy.main.ProxyServer -myPort=12345 -host=localhost -port=3306 -user=perftool -pass=perftool -thr=$1 -conn=$2 -run=$TOTALRUNTIME"
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; mv results.html $RESULTFOLDER/proxy$1-$2_$3_crazy_results.html"
	echo "***** $3 crazy proxy test with $1-$2 finished *****"
	memhogUnlock
}

function executeMysqlExperiment {
	restoreDB
	memhogLock 42949672960
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; taskset 0x00000f java -Xmx1g -cp \"lib/*\" com.linkedin.multitenant.main.RunStatusChecker -host=localhost -port=3306 -user=perftool -pass=perftool -run=$MYSQLRUNTIME -gran=$GRAN -out=$RESULTFOLDER/mysql_$1_crazy_mysqlSC.html" &
	echo "***** started mysql status checker *****"
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; taskset 0x00000f java -Xmx8g -cp \"lib/*\" -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$RESULTFOLDER/mysql_$1_crazy_perfgc.txt com.linkedin.multitenant.main.RunExperiment -plan=$PLANFOLDER/mysqlPlan_$1_crazy.xml -wait=$CLIENTSWAIT"
	ssh $USERNAME@$REMOTEHOST "cd $WORKFOLDER; mv results.html $RESULTFOLDER/mysql_$1_crazy_results.html"
	echo "***** $1 crazy mysql test finished *****"
	memhogUnlock
}

function runExp {
	mysqlStop
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo cp $CONFIGSFOLDER/$1.cnf /etc/my.cnf"
	mysqlStart

	executeMysqlExperiment mid
	executeProxyExperiment 200 50 mid
	executeProxyExperiment 200 100 mid

	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo cp -rp $RESULTFOLDER $WORKFOLDER/$1Results"
	echo $USERPASS | ssh -t -t $USERNAME@$REMOTEHOST "sudo rm $RESULTFOLDER/*"
}

runExp bp8
runExp bp4
runExp bp2
runExp bp1
runExp thr16
runExp thr8
