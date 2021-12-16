PROJ_ROOT=$(git rev-parse --show-toplevel)
if [ -z $ISPN_HOME ]; then
    ISPN_VER=${ISPN_VER:='13.0.3.Final'}
    ISPN_HOME=$(echo $PROJ_ROOT/infinispan-server-$ISPN_VER)
fi
$ISPN_HOME/bin/server.sh -c infinispan-app.xml
